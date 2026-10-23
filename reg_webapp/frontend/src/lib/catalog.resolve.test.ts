// Unit tests for the SHARED binding-resolution path (catalog.resolveBindingAt) —
// the single source of truth for the subject-page staged picker's resolve-once-at-
// pick-time adds (the #991 write-once model). Stubs the `states` read at the HTTP
// boundary, so the resolve is covered without a backend.
import { describe, expect, it } from "vitest";
import { stubApi } from "./api-test-helpers";
import { bindingFieldsFromResolution, resolveBindingAt } from "./catalog";
import { state } from "./catalog-test-helpers";

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
    // Fails if resolveBindingAt stops reading the states at the pick's period and
    // variant (any other request is a 404) or resolves co-existing columns to one
    // type.
    stubApi((url) =>
      url.pathname === "/api/states/scb/lisa/yrke" &&
      url.searchParams.get("period") === "2015" &&
      url.searchParams.get("variant") === "v1"
        ? { items: states, next_cursor: null }
        : undefined,
    );
    const r = await resolveBindingAt("scb/lisa/yrke", "2015", ["v1"]);
    expect(r.kind).toBe("ambiguous");
    if (r.kind === "ambiguous") {
      expect(r.fqid).toBe("scb/lisa/yrke");
      expect(r.states).toHaveLength(2);
    }
  });

  it("resolves a family source over its members, typed by the head-most member (#1840)", async () => {
    // LISA-shaped: one column handed from 16+ (to 2009) to its successor 15+, and a
    // concurrent variant's co-existing column the source does not reach.
    const states = [
      state({
        variant: "individer-16plus",
        delivery_column_name: "DispInk",
        data_type: "char",
        valid_from: "2004-01-01",
        valid_to: "2009-12-31",
      }),
      state({
        variant: "individer-15plus",
        delivery_column_name: "DispInk",
        data_type: "int",
        valid_from: "2010-01-01",
        valid_to: "2012-12-31",
      }),
      state({
        variant: "foretag",
        delivery_column_name: "DispInkF",
        data_type: "char",
        valid_from: "2004-01-01",
        valid_to: "2012-12-31",
      }),
    ];
    // Fails if the resolve keeps the per-variant filter (the 16+ years go
    // unresolved under a head-written source), lets the concurrent variant in
    // (ambiguous), or types the binding from the older member.
    stubApi((url) =>
      url.pathname === "/api/states/scb/lisa/dispink" &&
      url.searchParams.get("period") === "2008..2012" &&
      !url.searchParams.has("variant")
        ? { items: states, next_cursor: null }
        : undefined,
    );
    expect(
      await resolveBindingAt("scb/lisa/dispink", "2008..2012", [
        "individer-16plus",
        "individer-15plus",
      ]),
    ).toEqual({ kind: "derived", type: "numeric" });
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
