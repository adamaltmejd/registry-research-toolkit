// C1 (catalog→project handoff) STORE-level tests for addFromCatalog: find-or-create
// source by register_variant ALONE (#992), the concept-level duplicate guard, the
// period merge on a found source, the period prefill + #312 name prefill on a created
// source, and the resolve-ONCE → final-fields mapping (the #991 write-once model — no
// re-derivation engine). Mocks `./api`'s getCatalogNode (the resolve the shared
// `resolveBindingAt` path fetches) so a derive returns canned states; drives the real
// (now async) store and asserts the committed draft.
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { StatesResponse, VariableStateModel } from "./api";
import { getCatalogNode } from "./api";
import { type CatalogAddPayload, projectStore } from "./project_store.svelte";

vi.mock("./api", async (importOriginal) => {
  const actual = await importOriginal<typeof import("./api")>();
  return { ...actual, getCatalogNode: vi.fn() };
});

function vstate(over: Partial<VariableStateModel>): VariableStateModel {
  return {
    state_id: 1,
    variant: "v1",
    variant_label: null,
    register_variant_id: 1,
    valid_from: "2010-01-01",
    valid_to: "2020-12-31",
    data_type: null,
    data_length: null,
    delivery_column_name: null,
    source_register_text: null,
    value_set_version_label: "",
    value_set_id: null,
    value_set: null,
    is_identifier: false,
    classification_slug: null,
    ...over,
  };
}
function statesResp(states: VariableStateModel[]): StatesResponse {
  return { states } as unknown as StatesResponse;
}

const SEED = { reg_meta_version: "reg_meta/v1.0.0", steward: "global" };

/** A representation-less catalog add of `scb/lisa/kon` at variant v1, period 2018. */
function konPayload(over: Partial<CatalogAddPayload> = {}): CatalogAddPayload {
  return {
    registerVariant: "scb/lisa/v1",
    variable: "scb/lisa/kon",
    representation: null,
    resolvedPeriod: "2018",
    ...over,
  };
}

// The store is a module singleton with no public "reset to null", so the PRISTINE-
// path test runs FIRST against the module's initial null draft; every later case
// starts with an explicit `newProject(SEED)` so it never depends on the leftover.
beforeEach(() => {
  vi.mocked(getCatalogNode).mockReset();
});

describe("addFromCatalog (C1)", () => {
  it("creates the untitled project implicitly when the store is pristine, then adds + resolves", async () => {
    // The very first add runs against the module's initial null draft (pristine).
    vi.mocked(getCatalogNode).mockResolvedValue(
      statesResp([
        vstate({
          delivery_column_name: "Kon",
          value_set_id: 7, // categorical (a value set present)
        }),
      ]),
    );

    const result = await projectStore.addFromCatalog(konPayload(), SEED);
    expect(result).toEqual({
      status: "added",
      createdSource: true,
      sourceName: "LISA",
    });

    // The project was created from the seed.
    expect(projectStore.draft).not.toBeNull();
    expect(projectStore.draft?.reg_meta_version).toBe("reg_meta/v1.0.0");
    expect(projectStore.draft?.steward).toBe("global");

    // The source carries the register_variant + the period prefilled from 2018
    // (a single year → the number arm) + the #312 name prefill (register slug
    // uppercased).
    const source = projectStore.draft?.sources[0];
    expect(source?.register_variant).toBe("scb/lisa/v1");
    expect(source?.period).toBe(2018);
    expect(source?.name).toBe("LISA");

    // The binding was appended with the FINAL resolved fields (type + display name
    // from the resolved state — categorical, "Kon").
    expect(source?.bindings[0]).toMatchObject({
      variable: "scb/lisa/kon",
      type: "categorical",
      display_name: "Kon",
    });

    // The resolve hit the SOURCE's (period, variant).
    expect(getCatalogNode).toHaveBeenCalledWith("scb/lisa/kon", {
      period: "2018",
      variant: "v1",
    });
  });

  it("appends into an EXISTING matching source (a second variable from the same variant)", async () => {
    projectStore.newProject(SEED);
    await projectStore.addFromCatalog(konPayload(), SEED);
    vi.mocked(getCatalogNode).mockClear();

    vi.mocked(getCatalogNode).mockResolvedValue(
      statesResp([vstate({ delivery_column_name: "Lon", data_type: "int" })]),
    );

    const result = await projectStore.addFromCatalog(
      konPayload({ variable: "scb/lisa/lon" }),
      SEED,
    );
    // No new source — appended to the existing one.
    expect(result).toEqual({
      status: "added",
      createdSource: false,
      sourceName: "LISA",
    });
    expect(projectStore.draft?.sources).toHaveLength(1);
    expect(projectStore.draft?.sources[0].bindings).toHaveLength(2);
    expect(projectStore.draft?.sources[0].bindings[1]).toMatchObject({
      variable: "scb/lisa/lon",
      type: "numeric",
    });
  });

  it("the duplicate guard refuses a second add of the same fqid (already in project)", async () => {
    projectStore.newProject(SEED);
    vi.mocked(getCatalogNode).mockResolvedValue(
      statesResp([vstate({ delivery_column_name: "Kon", value_set_id: 7 })]),
    );

    const first = await projectStore.addFromCatalog(konPayload(), SEED);
    expect(first.status).toBe("added");
    expect(projectStore.draft?.sources[0].bindings).toHaveLength(1);

    // Adding the SAME variable again is a no-op → already-present.
    const second = await projectStore.addFromCatalog(konPayload(), SEED);
    expect(second.status).toBe("already-present");
    expect(projectStore.draft?.sources[0].bindings).toHaveLength(1);
  });

  it("a representation-specific add only collides with the SAME representation", async () => {
    projectStore.newProject(SEED);
    // The resolve sees two co-existing columns (ambiguous); the page pinned Ssyk3.
    vi.mocked(getCatalogNode).mockResolvedValue(
      statesResp([
        vstate({
          state_id: 1,
          delivery_column_name: "Ssyk3",
          value_set_id: 1,
          value_set: [{ code: "1", label: "a" }],
          value_set_version_label: "3-digit",
        }),
        vstate({
          state_id: 2,
          delivery_column_name: "Ssyk4",
          value_set_id: 2,
          value_set: [{ code: "11", label: "b" }],
          value_set_version_label: "4-digit",
        }),
      ]),
    );

    const a = await projectStore.addFromCatalog(
      konPayload({ variable: "scb/lisa/ssyk", representation: "Ssyk3" }),
      SEED,
    );
    expect(a.status).toBe("added");
    expect(projectStore.draft?.sources[0].bindings[0]).toMatchObject({
      representation: "Ssyk3",
      // The ambiguous resolution pinned the chosen column → its derived type +
      // delivery-column display name.
      type: "categorical",
      display_name: "Ssyk3",
    });

    // The OTHER representation (Ssyk4) is a distinct extraction → added, not a dup.
    const b = await projectStore.addFromCatalog(
      konPayload({ variable: "scb/lisa/ssyk", representation: "Ssyk4" }),
      SEED,
    );
    expect(b.status).toBe("added");
    expect(projectStore.draft?.sources[0].bindings).toHaveLength(2);
    expect(projectStore.draft?.sources[0].bindings[1]?.representation).toBe(
      "Ssyk4",
    );

    // Re-adding Ssyk3 IS a duplicate now.
    const c = await projectStore.addFromCatalog(
      konPayload({ variable: "scb/lisa/ssyk", representation: "Ssyk3" }),
      SEED,
    );
    expect(c.status).toBe("already-present");
  });

  it("a created source's name prefill suffixes _2 when the default is taken (#312)", async () => {
    projectStore.newProject(SEED);
    // A user-named source already holds "LISA" (a different variant, so the
    // catalog add creates a NEW source rather than appending).
    await projectStore.addFromCatalog(
      konPayload({ registerVariant: "scb/lisa/v2", variable: "scb/lisa/kon" }),
      SEED,
    );
    expect(projectStore.draft?.sources[0]?.name).toBe("LISA");
    vi.mocked(getCatalogNode).mockClear();
    vi.mocked(getCatalogNode).mockResolvedValue(
      statesResp([vstate({ delivery_column_name: "Kon", value_set_id: 7 })]),
    );

    // A DIFFERENT variant → a new source; its prefill collides with LISA → LISA_2.
    const result = await projectStore.addFromCatalog(konPayload(), SEED);
    expect(result).toEqual({
      status: "added",
      createdSource: true,
      sourceName: "LISA_2",
    });
    expect(projectStore.draft?.sources[1]?.name).toBe("LISA_2");
  });

  it("a created source with NO resolved period is left period-unset (blank-type binding, no fetch)", async () => {
    projectStore.newProject(SEED);
    // resolvedPeriod null → the source period stays unset; resolveBindingAt returns
    // unresolved WITHOUT a fetch (period-unset) → the binding lands with a BLANK
    // type (not "opaque") so the backend Validate flags the unresolved add rather
    // than passing a silently-valid opaque binding (review Fix 4).
    const result = await projectStore.addFromCatalog(
      konPayload({ resolvedPeriod: null }),
      SEED,
    );
    expect(result).toEqual({
      status: "added",
      createdSource: true,
      sourceName: "LISA",
    });
    expect(projectStore.draft?.sources[0].period).toBe("");
    expect(projectStore.draft?.sources[0].bindings[0]).toMatchObject({
      variable: "scb/lisa/kon",
      type: "",
    });
    // No fetch — the period-unset short-circuit in resolveBindingAt.
    expect(getCatalogNode).not.toHaveBeenCalled();
  });

  it("find-or-create by register_variant ALONE: a same-variant add MERGES its window into the existing source (#992)", async () => {
    projectStore.newProject(SEED);
    vi.mocked(getCatalogNode).mockResolvedValue(
      statesResp([vstate({ delivery_column_name: "Lon", data_type: "int" })]),
    );

    // An existing source at 2010 + an add of a DIFFERENT variable of the SAME
    // variant at a disjoint 2018 window: keyed on register_variant ALONE, the add
    // lands on the SAME source and EXTENDS its period to the #307 list form (no
    // second source — the #992 behavior change from the old (variant, period) key).
    await projectStore.addFromCatalog(
      konPayload({ variable: "scb/lisa/kon", resolvedPeriod: "2010" }),
      SEED,
    );
    await projectStore.addFromCatalog(
      konPayload({ variable: "scb/lisa/lon", resolvedPeriod: "2018" }),
      SEED,
    );

    expect(projectStore.draft?.sources).toHaveLength(1);
    expect(projectStore.draft?.sources[0].period).toEqual([2010, 2018]);
    expect(
      projectStore.draft?.sources[0].bindings.map((b) => b.variable),
    ).toEqual(["scb/lisa/kon", "scb/lisa/lon"]);
    // The SECOND add's binding resolves at the source's MERGED period (the wire of
    // the [2010, 2018] list) — the resolve reflects the extended coordinate.
    expect(getCatalogNode).toHaveBeenLastCalledWith("scb/lisa/lon", {
      period: "2010,2018",
      variant: "v1",
    });
  });

  it("collapse case: page pins a column but the SOURCE period is single-rep → re-add is a duplicate (MAJOR 2)", async () => {
    projectStore.newProject(SEED);
    // At the SOURCE's period the concept resolves to a SINGLE column (single-rep) —
    // `resolveBindingAt` returns `derived` (unambiguous). The page saw it as multi-rep
    // and pinned "Ssyk3", but a `derived` resolution is UNAMBIGUOUS, so the mapping
    // DROPS the payload column → representation null (review Fix 3, the #991
    // null-when-unambiguous convention).
    vi.mocked(getCatalogNode).mockResolvedValue(
      statesResp([
        vstate({
          delivery_column_name: "Ssyk3",
          value_set_id: 1,
          value_set: [{ code: "1", label: "a" }],
          value_set_version_label: "3-digit",
        }),
      ]),
    );

    const a = await projectStore.addFromCatalog(
      konPayload({ variable: "scb/lisa/ssyk", representation: "Ssyk3" }),
      SEED,
    );
    expect(a.status).toBe("added");
    // Single-column derived add → representation null (unambiguous), NOT the pinned
    // page column.
    expect(
      projectStore.draft?.sources[0].bindings[0]?.representation,
    ).toBeNull();

    // Re-adding is a duplicate: the stored null representation matches ANY payload
    // representation under the null-either-side rule (the page-pin-vs-source-derive
    // desync the concept-level guard catches).
    const b = await projectStore.addFromCatalog(
      konPayload({ variable: "scb/lisa/ssyk", representation: "Ssyk3" }),
      SEED,
    );
    expect(b.status).toBe("already-present");
    expect(projectStore.draft?.sources[0].bindings).toHaveLength(1);
  });

  it("an already-present add to a FOUND source is a TRUE no-op: period unchanged AND validation not cleared", async () => {
    projectStore.newProject(SEED);
    vi.mocked(getCatalogNode).mockResolvedValue(
      statesResp([vstate({ delivery_column_name: "Kon", value_set_id: 7 })]),
    );
    // First add creates+populates the source at period 2018.
    await projectStore.addFromCatalog(konPayload(), SEED);
    const periodBefore = projectStore.draft?.sources[0].period;
    expect(periodBefore).toBe(2018);

    // Establish a GREEN validation so we can prove the no-op does NOT clear it
    // (setDraft would null `validation` and flip `validatedClean` off, disabling
    // the order download while the UI reports "already there").
    vi.stubGlobal(
      "fetch",
      vi.fn(async () => ({
        ok: true,
        status: 200,
        json: async () => ({ ok: true, issues: [] }),
      })),
    );
    await projectStore.validate();
    expect(projectStore.validation?.ok).toBe(true);
    expect(projectStore.validatedClean).toBe(true);

    // Re-add the SAME (variable + representation) at a DIFFERENT (disjoint) period.
    // A pre-fix found path would have merged the period AND cleared validation
    // before the duplicate guard; the fix guards FIRST, so this is a zero-mutation
    // no-op.
    const result = await projectStore.addFromCatalog(
      konPayload({ resolvedPeriod: "2019" }),
      SEED,
    );
    expect(result).toEqual({
      status: "already-present",
      createdSource: false,
      sourceName: "LISA",
    });
    // Period byte-identical (NOT widened to the [2018, 2019] merge).
    expect(projectStore.draft?.sources[0].period).toBe(periodBefore);
    // Validation survived — the download gate stays open.
    expect(projectStore.validation?.ok).toBe(true);
    expect(projectStore.validatedClean).toBe(true);
    vi.unstubAllGlobals();
  });

  it("keeps a folded-rename null representation null on a derived resolution (does not overwrite with a resolved column)", async () => {
    projectStore.newProject(SEED);
    // A folded sequential rename intentionally commits a NULL representation. The
    // resolve is `derived` (single column) with a delivery column "Lon86" — the
    // mapping must NOT overwrite the payload's null representation with it.
    vi.mocked(getCatalogNode).mockResolvedValue(
      statesResp([vstate({ delivery_column_name: "Lon86", data_type: "int" })]),
    );
    await projectStore.addFromCatalog(
      konPayload({ variable: "scb/lisa/lon", representation: null }),
      SEED,
    );
    const binding = projectStore.draft?.sources[0].bindings[0];
    expect(binding?.representation).toBeNull();
    // The display default still comes from the resolved column.
    expect(binding).toMatchObject({ type: "numeric", display_name: "Lon86" });
  });

  it("discards a stale add when the draft is REPLACED mid-resolve, even when the replacement HAS a matching source (review Fix 1)", async () => {
    // The load-bearing case the `draft !== target` guard exists for: the
    // replacement draft ALREADY carries a source on the SAME register_variant as
    // the in-flight add, so the post-await re-find (`landIndex`) SUCCEEDS. Without
    // the guard, the stale binding would append into the WRONG (replaced) project's
    // matching source. An empty replacement draft would be caught by the pre-existing
    // `landIndex < 0` early return instead, so it can't discriminate the guard.
    projectStore.newProject(SEED); // draft A — the project the stale add starts against.

    // Hold the in-flight resolve open with a deferred promise so we can replace the
    // draft MID-FLIGHT. `getCatalogNode` is the seam `resolveBindingAt` awaits; only
    // THIS first call is deferred — the later populate of draft B resolves normally.
    let resolveFetch: (value: StatesResponse) => void = () => {};
    const pending = new Promise<StatesResponse>((res) => {
      resolveFetch = res;
    });
    vi.mocked(getCatalogNode).mockReturnValueOnce(pending);

    // Start the add of scb/lisa/lon on the scb/lisa/v1 variant against draft A; it
    // captures `target = A` and blocks on the pending resolve.
    const addPromise = projectStore.addFromCatalog(
      konPayload({ variable: "scb/lisa/lon" }),
      SEED,
    );

    // Replace the draft MID-FLIGHT with a brand-new project B, then populate B with a
    // source on the SAME register_variant (scb/lisa/v1) carrying a DIFFERENT variable
    // (kon). This resolve settles immediately (the once-mock above is exhausted).
    // The mutators REPLACE the draft object (immutable spread), so capture B's
    // identity AFTER the populate add — that is the object the in-flight add must not
    // touch.
    projectStore.newProject(SEED); // draft B
    vi.mocked(getCatalogNode).mockResolvedValue(
      statesResp([vstate({ delivery_column_name: "Kon", value_set_id: 7 })]),
    );
    await projectStore.addFromCatalog(konPayload(), SEED);
    const replacedDraft = projectStore.draft;
    // B now has one scb/lisa/v1 source with exactly one binding (kon).
    expect(replacedDraft?.sources).toHaveLength(1);
    expect(replacedDraft?.sources[0].register_variant).toBe("scb/lisa/v1");
    expect(replacedDraft?.sources[0].bindings).toHaveLength(1);

    // Now let the STALE resolve settle. The re-find WOULD succeed (B has a matching
    // scb/lisa/v1 source), so only the `draft !== target` guard stops the stale lon
    // binding from landing in B.
    resolveFetch(
      statesResp([vstate({ delivery_column_name: "Lon", data_type: "int" })]),
    );
    const result = await addPromise;

    // The stale add is discarded (no-op).
    expect(result.status).toBe("already-present");
    // B is untouched: still the same draft, its scb/lisa/v1 source did NOT gain the
    // stale lon binding (still exactly one binding — kon). Removing the guard makes
    // this assertion FAIL: landIndex would find B's source and append lon → 2 bindings.
    expect(projectStore.draft).toBe(replacedDraft);
    expect(projectStore.draft?.sources).toHaveLength(1);
    expect(projectStore.draft?.sources[0].bindings).toHaveLength(1);
    expect(projectStore.draft?.sources[0].bindings[0]?.variable).toBe(
      "scb/lisa/kon",
    );
  });
});
