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
    variant_label: null,
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

  it("a #307 list period re-derives through the comma wire (#340: no period-unset fallback)", async () => {
    freshDraftWithBinding();
    projectStore.applyPickedBinding(0, 0, {
      variable: "scb/lisa/lon",
      type: "opaque",
      displayNameDefault: null,
      status: "unresolved",
      reason: "period-unset",
    });
    vi.mocked(getCatalogNode).mockResolvedValue(
      statesResp([vstate({ delivery_column_name: "Lon", data_type: "int" })]),
    );
    // Setting an interrupted-series period resolves like any other: the
    // catalog ?period= accepts the comma wire since #340 (the old
    // periodToResolveWire null → period-unset fallback is deleted).
    projectStore.updateSource(0, {
      period: [
        { from: 2005, to: 2010 },
        { from: 2015, to: 2020 },
      ],
    });
    await vi.waitFor(() => {
      expect(deriv0()?.status).toBe("derived");
      expect(binding0()?.type).toBe("numeric");
    });
    expect(vi.mocked(getCatalogNode)).toHaveBeenCalledWith(
      "scb/lisa/lon",
      expect.objectContaining({ period: "2005..2010,2015..2020" }),
    );
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

// ── Structural-mutation-during-in-flight-resolve races (reviewer BLOCKER) ─────
// rederiveSource captures positional (sourceIndex, bindingIndex); an async resolve
// (a network fetch, hundreds of ms) leaves a window where a remove can shift those
// indices. The fix is belt-AND-suspenders: (1) remove{Source,Binding} invalidate
// the source's rederiveGen so the in-flight pass is discarded; (2) applyResolution
// re-verifies the captured stable ids + variable FQID before any write. These tests
// use a DEFERRED-promise mock so the race is deterministic: hold every resolve open,
// perform the structural mutation, THEN release — and assert nothing mis-attributes.

/** A controllable mock: each getCatalogNode call returns a promise we resolve by
 * hand. `release(i, states)` settles the i-th call (in call order). */
function deferredResolveMock() {
  const settles: ((v: StatesResponse) => void)[] = [];
  vi.mocked(getCatalogNode).mockImplementation(
    () =>
      new Promise<StatesResponse>((resolve) => {
        settles.push(resolve);
      }) as unknown as ReturnType<typeof getCatalogNode>,
  );
  return {
    /** Number of in-flight (un-settled) resolve calls so far. */
    get count() {
      return settles.length;
    },
    /** Settle the i-th call with a single int-typed delivery column. */
    release(i: number, column = "Col") {
      settles[i](
        statesResp([
          vstate({ delivery_column_name: column, data_type: "int" }),
        ]),
      );
    },
    /** Settle every pending call. */
    releaseAll(column = "Col") {
      const resp = statesResp([
        vstate({ delivery_column_name: column, data_type: "int" }),
      ]);
      for (const s of settles) {
        s(resp);
      }
    },
  };
}

/** Build N sources, each with one binding pre-picked (variable set, derived
 * provenance) at period 2015, register scb/lisa/v1. Uses an immediate mock so the
 * setup picks resolve synchronously; the caller swaps in a deferred mock after. */
function buildSources(n: number): void {
  projectStore.newProject({
    reg_meta_version: "reg_meta/v1.0.0",
    steward: "global",
  });
  for (let i = 0; i < n; i++) {
    projectStore.addSource();
    projectStore.updateSource(i, { register_variant: "scb/lisa/v1" });
    projectStore.addBinding(i);
    // Record provenance directly (no fetch) so each binding has a derived baseline.
    projectStore.updateSource(i, { period: 2015 });
    projectStore.applyPickedBinding(i, 0, {
      variable: `scb/lisa/var${i}`,
      type: "numeric",
      displayNameDefault: `Col${i}`,
      status: "derived",
    });
  }
}

describe("structural mutation during an in-flight re-derive", () => {
  it("(a) remove-source-mid-flight does not write onto the shifted source", async () => {
    buildSources(3); // sources 0,1,2 each with a picked binding
    const mock = deferredResolveMock();

    // Kick off a re-derive of SOURCE 1 (change its period) — its binding's resolve
    // is now in flight (held by the deferred mock).
    projectStore.updateSource(1, { period: 2018 });
    await vi.waitFor(() => expect(mock.count).toBe(1));

    // The victim we must NOT clobber: source 2's binding. Snapshot its state.
    const src2VarBefore = projectStore.draft?.sources[2].bindings[0]?.variable;
    expect(src2VarBefore).toBe("scb/lisa/var2");

    // Remove source 0 → old source 1 shifts to index 0, old source 2 to index 1.
    projectStore.removeSource(0);

    // Release the in-flight resolve (it was dispatched for OLD index 1). The
    // identity re-check sees the stable id at index 1 no longer matches → DROP.
    mock.release(0, "NewCol");

    // Give microtasks a chance to flush, then assert nothing was mis-written.
    await Promise.resolve();
    await Promise.resolve();

    // What is NOW at index 1 (old source 2) keeps its own variable + derived col —
    // it was never the target of the in-flight resolve.
    expect(projectStore.draft?.sources[1].bindings[0]?.variable).toBe(
      "scb/lisa/var2",
    );
    expect(projectStore.draft?.sources[1].bindings[0]?.display_name).toBe(
      "Col2",
    );
    // And it was not re-marked by the dropped resolution.
    expect(projectStore.bindingDerivation(1, 0)?.status).toBe("derived");
  });

  it("(b) remove-binding-mid-flight does not write onto the shifted binding", async () => {
    // One source, three picked bindings.
    projectStore.newProject({
      reg_meta_version: "reg_meta/v1.0.0",
      steward: "global",
    });
    projectStore.addSource();
    projectStore.updateSource(0, { register_variant: "scb/lisa/v1" });
    for (let j = 0; j < 3; j++) {
      projectStore.addBinding(0);
      projectStore.updateSource(0, { period: 2015 });
      projectStore.applyPickedBinding(0, j, {
        variable: `scb/lisa/b${j}`,
        type: "numeric",
        displayNameDefault: `Col${j}`,
        status: "derived",
      });
    }
    const mock = deferredResolveMock();

    // Re-derive the whole source (period change) → 3 resolves in flight.
    projectStore.updateSource(0, { period: 2019 });
    await vi.waitFor(() => expect(mock.count).toBe(3));

    // Remove binding 0 → bindings 1,2 shift down to indices 0,1.
    projectStore.removeBinding(0, 0);

    // Release every in-flight resolve. The gen for this source was invalidated by
    // removeBinding, so all of them must be discarded (no shifted-binding writes).
    mock.releaseAll("NewCol");
    await Promise.resolve();
    await Promise.resolve();

    // The surviving bindings keep their own picked variables + display columns,
    // NOT a neighbour's resolution.
    expect(projectStore.draft?.sources[0].bindings[0]?.variable).toBe(
      "scb/lisa/b1",
    );
    expect(projectStore.draft?.sources[0].bindings[0]?.display_name).toBe(
      "Col1",
    );
    expect(projectStore.draft?.sources[0].bindings[1]?.variable).toBe(
      "scb/lisa/b2",
    );
    expect(projectStore.draft?.sources[0].bindings[1]?.display_name).toBe(
      "Col2",
    );
  });

  it("(c) the gen invalidation on removal discards in-flight resolutions", async () => {
    // One source, one picked binding; hold its re-derive in flight, then remove a
    // (second) binding to invalidate the source gen — the held resolve must no-op.
    projectStore.newProject({
      reg_meta_version: "reg_meta/v1.0.0",
      steward: "global",
    });
    projectStore.addSource();
    projectStore.updateSource(0, { register_variant: "scb/lisa/v1" });
    projectStore.addBinding(0);
    projectStore.updateSource(0, { period: 2015 });
    projectStore.applyPickedBinding(0, 0, {
      variable: "scb/lisa/keep",
      type: "numeric",
      displayNameDefault: "KeepCol",
      status: "derived",
    });
    projectStore.addBinding(0); // a second, un-picked binding (index 1)

    const mock = deferredResolveMock();
    projectStore.updateSource(0, { period: 2020 });
    // Only binding 0 has a variable → exactly one in-flight resolve.
    await vi.waitFor(() => expect(mock.count).toBe(1));

    // Remove the second (empty) binding → invalidates the source's gen.
    projectStore.removeBinding(0, 1);

    // Release the held resolve for binding 0 with a DIFFERENT column; the gen guard
    // must discard it, leaving binding 0's original derived values intact.
    mock.release(0, "ChangedCol");
    await Promise.resolve();
    await Promise.resolve();

    expect(projectStore.draft?.sources[0].bindings[0]?.display_name).toBe(
      "KeepCol",
    );
  });
});
