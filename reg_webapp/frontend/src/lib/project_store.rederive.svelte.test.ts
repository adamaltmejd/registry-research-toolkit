// B2 derived-vs-user-set + re-derive-on-period-change tests, at the STORE level
// (the clobber-vs-keep logic lives in project_store.svelte.ts → applyResolution).
// Mocks `./api`'s getCatalogNode so the shared resolveBindingAt path returns canned
// states; drives the real store mutators and asserts the draft + the per-binding
// derivation marker.
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { StatesResponse, VariableStateModel } from "./api";
import { getCatalogNode } from "./api";
import { projectStore } from "./project_store.svelte";

vi.mock("./api", async (importOriginal) => {
  const actual = await importOriginal<typeof import("./api")>();
  return { ...actual, getCatalogNode: vi.fn() };
});

function vstate(over: Partial<VariableStateModel>): VariableStateModel {
  return {
    state_id: 1,
    variant: "v",
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

/** A fresh draft with one source (register_variant scb/lisa/v1, period unset) and
 * one empty binding. */
function freshDraftWithBinding(): void {
  projectStore.newProject({
    reg_meta_version: "reg_meta/v1.0.0",
    steward: "global",
  });
  projectStore.addSource();
  projectStore.updateSource(0, { register_variant: "scb/lisa/v1" });
  projectStore.addBinding(0);
}

function binding0() {
  return projectStore.draft?.sources[0].bindings[0];
}
function deriv0() {
  return projectStore.bindingDerivation(0, 0);
}

beforeEach(() => {
  vi.mocked(getCatalogNode).mockReset();
});

describe("re-derive on period change (B2)", () => {
  it("pick with period unset stays opaque + unresolved; setting the period re-derives the real type", async () => {
    freshDraftWithBinding();
    // Pick with period UNSET → opaque fallback, unresolved marker (no fetch).
    projectStore.applyPickedBinding(0, 0, {
      variable: "scb/lisa/lon",
      type: "opaque",
      displayNameDefault: null,
      status: "unresolved",
      reason: "period-unset",
    });
    expect(binding0()?.type).toBe("opaque");
    expect(deriv0()?.status).toBe("unresolved");
    expect(deriv0()?.reason).toBe("period-unset");

    // Now set the period → the source re-derives every binding. reg_meta says the
    // variable is `int` here → numeric.
    vi.mocked(getCatalogNode).mockResolvedValue(
      statesResp([vstate({ delivery_column_name: "Lon", data_type: "int" })]),
    );
    projectStore.updateSource(0, { period: 2015 });

    await vi.waitFor(() => {
      expect(binding0()?.type).toBe("numeric");
      expect(deriv0()?.status).toBe("derived");
    });
    // The display-name default auto-filled (the field was blank → ours).
    expect(binding0()?.display_name).toBe("Lon");
  });

  it("a user-edited type is KEPT on re-derive, with a non-blocking mismatch hint", async () => {
    freshDraftWithBinding();
    // Pick at a period that derives `numeric`.
    vi.mocked(getCatalogNode).mockResolvedValue(
      statesResp([vstate({ delivery_column_name: "Lon", data_type: "int" })]),
    );
    projectStore.updateSource(0, { period: 2015 });
    // Resolve happens after the pick records provenance — drive the pick directly
    // at the set period so provenance carries derivedType numeric.
    projectStore.applyPickedBinding(0, 0, {
      variable: "scb/lisa/lon",
      type: "numeric",
      displayNameDefault: "Lon",
      status: "derived",
    });
    expect(binding0()?.type).toBe("numeric");

    // The user hand-edits the type to `id`.
    projectStore.updateBinding(0, 0, { type: "id" });
    expect(binding0()?.type).toBe("id");

    // Change the period — reg_meta still derives `numeric`, but the user diverged.
    projectStore.updateSource(0, { period: 2016 });
    await vi.waitFor(() => {
      // The user's `id` is KEPT (never clobbered).
      expect(binding0()?.type).toBe("id");
      // A non-blocking mismatch hint surfaces.
      expect(deriv0()?.mismatch).toEqual({ field: "type", derived: "numeric" });
    });
  });

  it("a user-edited display name survives a period change", async () => {
    freshDraftWithBinding();
    vi.mocked(getCatalogNode).mockResolvedValue(
      statesResp([vstate({ delivery_column_name: "Lon", data_type: "int" })]),
    );
    projectStore.updateSource(0, { period: 2015 });
    projectStore.applyPickedBinding(0, 0, {
      variable: "scb/lisa/lon",
      type: "numeric",
      displayNameDefault: "Lon",
      status: "derived",
    });
    // User renames the display name.
    projectStore.updateBinding(0, 0, { display_name: "Monthly income" });

    // Period changes; reg_meta default is still "Lon".
    projectStore.updateSource(0, { period: 2016 });
    await vi.waitFor(() => {
      // The user's display name survives.
      expect(binding0()?.display_name).toBe("Monthly income");
      expect(deriv0()?.status).toBe("derived");
    });
  });

  it("clearing the period marks the binding unresolved (no silent opaque)", async () => {
    freshDraftWithBinding();
    vi.mocked(getCatalogNode).mockResolvedValue(
      statesResp([vstate({ delivery_column_name: "Lon", data_type: "int" })]),
    );
    projectStore.updateSource(0, { period: 2015 });
    projectStore.applyPickedBinding(0, 0, {
      variable: "scb/lisa/lon",
      type: "numeric",
      displayNameDefault: "Lon",
      status: "derived",
    });

    // Clear the period → resolution impossible.
    projectStore.updateSource(0, { period: "" });
    await vi.waitFor(() => {
      expect(deriv0()?.status).toBe("unresolved");
      expect(deriv0()?.reason).toBe("period-unset");
    });
    // The derived type is NOT clobbered to opaque (the validator is the authority);
    // it stays at the last-derived numeric, just marked unresolved.
    expect(binding0()?.type).toBe("numeric");
  });
});
