// C1 (catalog→project handoff) STORE-level tests for addFromCatalog: find-vs-create
// source, the duplicate guard, the period prefill on a created source, and the
// derive-at-source-period applied through the shared resolveBindingAt path. Mocks
// `./api`'s getCatalogNode (the resolve the shared path fetches) so a derive returns
// canned states; drives the real store and asserts the draft + per-binding marker.
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
    register_variant_id: 1,
    valid_from: "2010-01-01",
    valid_to: "2020-12-31",
    data_type: null,
    data_length: null,
    delivery_column_name: null,
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
  it("creates the untitled project implicitly when the store is pristine, then adds + derives", async () => {
    // The very first add runs against the module's initial null draft (pristine).
    vi.mocked(getCatalogNode).mockResolvedValue(
      statesResp([
        vstate({
          delivery_column_name: "Kon",
          value_set_id: 7, // categorical (a value set present)
        }),
      ]),
    );

    const result = projectStore.addFromCatalog(konPayload(), SEED);
    expect(result).toEqual({ status: "added", createdSource: true });

    // The project was created from the seed.
    expect(projectStore.draft).not.toBeNull();
    expect(projectStore.draft?.reg_meta_version).toBe("reg_meta/v1.0.0");
    expect(projectStore.draft?.steward).toBe("global");

    // The source carries the register_variant + the period prefilled from 2018
    // (a single year → the number arm).
    const source = projectStore.draft?.sources[0];
    expect(source?.register_variant).toBe("scb/lisa/v1");
    expect(source?.period).toBe(2018);

    // The binding was appended with the variable; the derive lands the type +
    // display name from the resolved state (categorical, "Kon").
    expect(source?.bindings[0]?.variable).toBe("scb/lisa/kon");
    await vi.waitFor(() => {
      expect(projectStore.draft?.sources[0].bindings[0]?.type).toBe(
        "categorical",
      );
      expect(projectStore.draft?.sources[0].bindings[0]?.display_name).toBe(
        "Kon",
      );
      expect(projectStore.bindingDerivation(0, 0)?.status).toBe("derived");
    });

    // The resolve hit the SOURCE's (period, variant).
    expect(getCatalogNode).toHaveBeenCalledWith("scb/lisa/kon", {
      period: "2018",
      variant: "v1",
    });
  });

  it("appends into an EXISTING matching source (a second variable from the same variant)", async () => {
    // Project already has the kon source (from the previous test's pristine create —
    // but tests must be order-independent, so build it explicitly here).
    projectStore.newProject(SEED);
    projectStore.addSource();
    projectStore.updateSource(0, {
      register_variant: "scb/lisa/v1",
      period: 2018,
    });

    vi.mocked(getCatalogNode).mockResolvedValue(
      statesResp([vstate({ delivery_column_name: "Lon", data_type: "int" })]),
    );

    const result = projectStore.addFromCatalog(
      konPayload({ variable: "scb/lisa/lon" }),
      SEED,
    );
    // No new source — appended to the existing one.
    expect(result).toEqual({ status: "added", createdSource: false });
    expect(projectStore.draft?.sources).toHaveLength(1);
    expect(projectStore.draft?.sources[0].bindings).toHaveLength(1);
    expect(projectStore.draft?.sources[0].bindings[0]?.variable).toBe(
      "scb/lisa/lon",
    );
    await vi.waitFor(() =>
      expect(projectStore.draft?.sources[0].bindings[0]?.type).toBe("numeric"),
    );
  });

  it("the duplicate guard refuses a second add of the same fqid (already in project)", async () => {
    projectStore.newProject(SEED);
    projectStore.addSource();
    projectStore.updateSource(0, {
      register_variant: "scb/lisa/v1",
      period: 2018,
    });
    vi.mocked(getCatalogNode).mockResolvedValue(
      statesResp([vstate({ delivery_column_name: "Kon", value_set_id: 7 })]),
    );

    const first = projectStore.addFromCatalog(konPayload(), SEED);
    expect(first.status).toBe("added");
    await vi.waitFor(() =>
      expect(projectStore.draft?.sources[0].bindings).toHaveLength(1),
    );

    // Adding the SAME variable again is a no-op → already-present.
    const second = projectStore.addFromCatalog(konPayload(), SEED);
    expect(second.status).toBe("already-present");
    expect(projectStore.draft?.sources[0].bindings).toHaveLength(1);
  });

  it("a representation-specific add only collides with the SAME representation", async () => {
    projectStore.newProject(SEED);
    projectStore.addSource();
    projectStore.updateSource(0, {
      register_variant: "scb/lisa/v1",
      period: 2018,
    });
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

    const a = projectStore.addFromCatalog(
      konPayload({ variable: "scb/lisa/ssyk", representation: "Ssyk3" }),
      SEED,
    );
    expect(a.status).toBe("added");
    await vi.waitFor(() =>
      expect(projectStore.draft?.sources[0].bindings[0]?.representation).toBe(
        "Ssyk3",
      ),
    );

    // The OTHER representation (Ssyk4) is a distinct extraction → added, not a dup.
    const b = projectStore.addFromCatalog(
      konPayload({ variable: "scb/lisa/ssyk", representation: "Ssyk4" }),
      SEED,
    );
    expect(b.status).toBe("added");
    expect(projectStore.draft?.sources[0].bindings).toHaveLength(2);
    await vi.waitFor(() =>
      expect(projectStore.draft?.sources[0].bindings[1]?.representation).toBe(
        "Ssyk4",
      ),
    );

    // Re-adding Ssyk3 IS a duplicate now.
    const c = projectStore.addFromCatalog(
      konPayload({ variable: "scb/lisa/ssyk", representation: "Ssyk3" }),
      SEED,
    );
    expect(c.status).toBe("already-present");
  });

  it("a created source with NO resolved period is left period-unset (the binding marks unresolved)", async () => {
    projectStore.newProject(SEED);
    // resolvedPeriod null → the source period stays unset; resolveBindingAt returns
    // unresolved WITHOUT a fetch (period-unset).
    const result = projectStore.addFromCatalog(
      konPayload({ resolvedPeriod: null }),
      SEED,
    );
    expect(result).toEqual({ status: "added", createdSource: true });
    expect(projectStore.draft?.sources[0].period).toBe("");
    await vi.waitFor(() => {
      expect(projectStore.bindingDerivation(0, 0)?.status).toBe("unresolved");
      expect(projectStore.bindingDerivation(0, 0)?.reason).toBe("period-unset");
    });
    // No fetch — the period-unset short-circuit in resolveBindingAt.
    expect(getCatalogNode).not.toHaveBeenCalled();
  });

  it("re-derives at the SOURCE's period, not the page's, when an existing source has a different period", async () => {
    projectStore.newProject(SEED);
    projectStore.addSource();
    projectStore.updateSource(0, {
      register_variant: "scb/lisa/v1",
      period: 2010, // the source lives at 2010
    });
    vi.mocked(getCatalogNode).mockResolvedValue(
      statesResp([vstate({ delivery_column_name: "Lon", data_type: "int" })]),
    );

    // The catalog page resolved the variable at 2018, but the source is at 2010.
    projectStore.addFromCatalog(
      konPayload({ variable: "scb/lisa/lon", resolvedPeriod: "2018" }),
      SEED,
    );
    await vi.waitFor(() =>
      expect(projectStore.draft?.sources[0].bindings[0]?.type).toBe("numeric"),
    );
    // The resolve used the SOURCE's period (2010), not the page's (2018).
    expect(getCatalogNode).toHaveBeenCalledWith("scb/lisa/lon", {
      period: "2010",
      variant: "v1",
    });
  });
});
