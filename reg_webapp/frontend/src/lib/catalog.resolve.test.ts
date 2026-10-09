// Unit tests for the SHARED binding-resolution path (catalog.resolveBindingAt) —
// the single source of truth for the subject-page staged picker's resolve-once-at-
// pick-time adds (the #991 write-once model). Mocks `./api`'s getStates so the
// resolve branches (period-unset / no-states / derived / ambiguous) are covered
// without a backend. Kept out of the PURE catalog.*.test.ts files so they need no
// module mock.
import { beforeEach, describe, expect, it, vi } from "vitest";
import { getStates } from "./api";
import { bindingFieldsFromResolution, resolveBindingAt } from "./catalog";
import { state } from "./catalog-test-helpers";

vi.mock("./api", async (importOriginal) => {
  const actual = await importOriginal<typeof import("./api")>();
  return { ...actual, getStates: vi.fn() };
});

beforeEach(() => {
  vi.mocked(getStates).mockReset();
});

describe("resolveBindingAt", () => {
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
    // Fails if resolveBindingAt stops reading the `period` states facet or
    // resolves co-existing columns to one type (RUST_RUNTIME_SPEC.md package C).
    vi.mocked(getStates).mockResolvedValue(states);
    const r = await resolveBindingAt("scb/lisa/yrke", "2015", "v1");
    expect(r.kind).toBe("ambiguous");
    if (r.kind === "ambiguous") {
      expect(r.fqid).toBe("scb/lisa/yrke");
      expect(r.states).toHaveLength(2);
    }
  });
});

// The expectations below are EXACT objects, not `objectContaining`: every arm writes
// `representation` and leaves `display_name` absent (Y-76).
describe("bindingFieldsFromResolution", () => {
  it("takes the chosen column's type when the picker resolves an ambiguous pick", () => {
    expect(
      bindingFieldsFromResolution(
        "scb/iot/dispink",
        {
          kind: "ambiguous",
          fqid: "scb/iot/dispink",
          states: [
            state({ delivery_column_name: "CDISP", data_type: "char" }),
            state({ delivery_column_name: "CDISP5", data_type: "int" }),
          ],
        },
        "CDISP5",
      ),
    ).toEqual({
      variable: "scb/iot/dispink",
      type: "numeric",
      representation: "CDISP5",
    });
  });
});
