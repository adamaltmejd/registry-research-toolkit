// Unit tests for the SHARED binding-resolution path (catalog.resolveBindingAt) —
// the single source of truth for the store's resolve-once-at-pick-time
// (`addFromCatalog`, the #991 write-once model). Mocks `./api`'s getCatalogNode so the
// resolve branches (period-unset / no-states / derived / ambiguous) are covered
// without a backend. Kept out of the PURE catalog.test.ts so that file needs no
// module mock.
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { CatalogNode, StatesResponse, VariableStateModel } from "./api";
import { getCatalogNode } from "./api";
import { resolveBindingAt } from "./catalog";

vi.mock("./api", async (importOriginal) => {
  const actual = await importOriginal<typeof import("./api")>();
  return { ...actual, getCatalogNode: vi.fn() };
});

function state(over: Partial<VariableStateModel>): VariableStateModel {
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
    source_register_text: null,
    value_set_version_label: "",
    value_set_id: null,
    value_set: null,
    is_identifier: false,
    classification_slug: null,
    ...over,
  };
}

function statesResponse(states: VariableStateModel[]): StatesResponse {
  return { states } as unknown as StatesResponse;
}

beforeEach(() => {
  vi.mocked(getCatalogNode).mockReset();
});

describe("resolveBindingAt", () => {
  it("period-unset → unresolved WITHOUT a fetch", async () => {
    const r = await resolveBindingAt("scb/lisa/lon", null, "v1");
    expect(r).toEqual({ kind: "unresolved", reason: "period-unset" });
    expect(getCatalogNode).not.toHaveBeenCalled();
  });

  it("a covering single-rep state → derived (type + display default + null rep)", async () => {
    vi.mocked(getCatalogNode).mockResolvedValue(
      statesResponse([
        state({ delivery_column_name: "Lon", data_type: "int" }),
      ]),
    );
    const r = await resolveBindingAt("scb/lisa/lon", "2015", "v1");
    expect(r).toEqual({
      kind: "derived",
      type: "numeric",
      displayNameDefault: "Lon",
      representation: null,
    });
    // The (period, variant) rode the resolve query.
    expect(getCatalogNode).toHaveBeenCalledWith("scb/lisa/lon", {
      period: "2015",
      variant: "v1",
    });
  });

  it("no covering state → unresolved (no-states)", async () => {
    vi.mocked(getCatalogNode).mockResolvedValue(statesResponse([]));
    const r = await resolveBindingAt("scb/lisa/lon", "2015", "v1");
    expect(r).toEqual({ kind: "unresolved", reason: "no-states" });
  });

  it("a browsable node (no ?period resolve) → unresolved (not-a-leaf)", async () => {
    vi.mocked(getCatalogNode).mockResolvedValue({
      kind: "register",
      fqid: "scb/lisa",
      children: [],
    } as unknown as CatalogNode);
    const r = await resolveBindingAt("scb/lisa", "2015", "v1");
    expect(r).toEqual({ kind: "unresolved", reason: "not-a-leaf" });
  });

  it(">1 co-existing delivery column → ambiguous (deferred to the picker chooser)", async () => {
    // Two distinct columns with OVERLAPPING validity → coexisting → ambiguous.
    const states = [
      state({
        delivery_column_name: "Ssyk3",
        valid_from: "2010-01-01",
        valid_to: "2020-12-31",
      }),
      state({
        delivery_column_name: "Ssyk4",
        valid_from: "2010-01-01",
        valid_to: "2020-12-31",
      }),
    ];
    vi.mocked(getCatalogNode).mockResolvedValue(statesResponse(states));
    const r = await resolveBindingAt("scb/lisa/yrke", "2015", "v1");
    expect(r.kind).toBe("ambiguous");
    if (r.kind === "ambiguous") {
      expect(r.fqid).toBe("scb/lisa/yrke");
      expect(r.states).toHaveLength(2);
    }
  });

  it("omits the ?variant modifier when the variant seg is empty", async () => {
    vi.mocked(getCatalogNode).mockResolvedValue(
      statesResponse([state({ delivery_column_name: "X", data_type: "int" })]),
    );
    await resolveBindingAt("scb/lisa/lon", "2015", "");
    expect(getCatalogNode).toHaveBeenCalledWith("scb/lisa/lon", {
      period: "2015",
      variant: undefined,
    });
  });
});
