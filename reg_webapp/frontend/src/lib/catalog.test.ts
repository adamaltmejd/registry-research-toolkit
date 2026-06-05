import { describe, expect, it } from "vitest";
import type { CatalogNode, VariableStateModel } from "./api";
import {
  breadcrumbs,
  catalogHref,
  deriveType,
  fqidSegments,
  nodeLabel,
  registerPrefixOf,
  representationsFromStates,
  variantSeg,
} from "./catalog";

// Minimal VariableStateModel — only the fields deriveType/distinctVersions read.
function state(over: Partial<VariableStateModel>): VariableStateModel {
  return {
    state_id: 1,
    variant: "v",
    register_variant_id: 1,
    valid_from: "",
    valid_to: "",
    data_type: null,
    data_length: null,
    delivery_column_name: null,
    value_set_version_label: "",
    value_set_id: null,
    value_set: null,
    ...over,
  };
}

// Minimal fixtures for each `kind` arm — only the fields the helpers read.
const provider = {
  kind: "provider",
  fqid: "scb",
  name: "Statistics Sweden",
} as CatalogNode;
const register = {
  kind: "register",
  fqid: "scb/lisa",
  name: null,
  purpose: null,
  children: [],
} as unknown as CatalogNode;
const classification = {
  kind: "classification",
  fqid: "class/sun2020",
  name: "Education",
  short_name: "SUN",
} as unknown as CatalogNode;

describe("nodeLabel", () => {
  it("uses name when present, else falls back to fqid", () => {
    expect(nodeLabel(provider)).toBe("Statistics Sweden");
    expect(nodeLabel(register)).toBe("scb/lisa"); // name is null → fqid
    expect(nodeLabel(classification)).toBe("Education");
  });
});

describe("catalogHref", () => {
  it("mirrors the API path for an ASCII FQID (encoding is a no-op)", () => {
    expect(catalogHref("scb/lisa/kon")).toBe("/catalog/scb/lisa/kon");
    expect(catalogHref("")).toBe("/catalog");
  });

  it("percent-encodes reserved/non-ASCII chars per segment", () => {
    expect(catalogHref("scb/lisa/kön")).toBe("/catalog/scb/lisa/k%C3%B6n");
  });
});

describe("fqidSegments / breadcrumbs", () => {
  it("splits an fqid path into segments, [] for the root", () => {
    expect(fqidSegments("scb/lisa/kon")).toEqual(["scb", "lisa", "kon"]);
    expect(fqidSegments("")).toEqual([]);
  });

  it("builds a cumulative breadcrumb trail", () => {
    expect(breadcrumbs("scb/lisa/kon")).toEqual([
      { label: "scb", fqidPath: "scb" },
      { label: "lisa", fqidPath: "scb/lisa" },
      { label: "kon", fqidPath: "scb/lisa/kon" },
    ]);
  });
});

describe("registerPrefixOf / variantSeg", () => {
  it("splits a 3-seg register_variant into prefix + variant", () => {
    expect(registerPrefixOf("scb/lisa/individer")).toBe("scb/lisa");
    expect(variantSeg("scb/lisa/individer")).toBe("individer");
  });

  it("returns '' for a register_variant of the wrong shape", () => {
    expect(registerPrefixOf("scb")).toBe(""); // < 2 segments
    expect(registerPrefixOf("")).toBe("");
    expect(variantSeg("scb/lisa")).toBe(""); // not exactly 3 segments
    expect(variantSeg("")).toBe("");
  });
});

describe("deriveType", () => {
  it("no state → opaque", () => {
    expect(deriveType(undefined)).toBe("opaque");
  });

  it("a value set → categorical (overrides the storage token)", () => {
    expect(deriveType(state({ value_set_id: 5, data_type: "int" }))).toBe(
      "categorical",
    );
    expect(
      deriveType(
        state({
          value_set: [
            { code: "1", label: "x" },
          ] as VariableStateModel["value_set"],
          data_type: "char",
        }),
      ),
    ).toBe("categorical");
  });

  it("maps SQL storage tokens (stripping a trailing length)", () => {
    expect(deriveType(state({ data_type: "int" }))).toBe("numeric");
    expect(deriveType(state({ data_type: "decimal(10,2)" }))).toBe("numeric");
    expect(deriveType(state({ data_type: "date" }))).toBe("date");
    expect(deriveType(state({ data_type: "smalldatetime" }))).toBe("datetime");
    expect(deriveType(state({ data_type: "uniqueidentifier" }))).toBe("id");
  });

  it("maps the Swedish Datatyp tokens SOS delivers (case-insensitive)", () => {
    expect(deriveType(state({ data_type: "Heltal" }))).toBe("numeric");
    expect(deriveType(state({ data_type: "Decimaltal" }))).toBe("numeric");
    expect(deriveType(state({ data_type: "numerisk" }))).toBe("numeric");
    expect(deriveType(state({ data_type: "Datum" }))).toBe("date");
    expect(deriveType(state({ data_type: "Identifierare" }))).toBe("id");
  });

  it("unrecognized / empty storage token → opaque (user picks)", () => {
    expect(deriveType(state({ data_type: "alfanumerisk" }))).toBe("opaque");
    expect(deriveType(state({ data_type: "Sträng (text)" }))).toBe("opaque");
    expect(deriveType(state({ data_type: "" }))).toBe("opaque");
    expect(deriveType(state({ data_type: "<undefined>" }))).toBe("opaque");
  });
});

describe("representationsFromStates", () => {
  it("returns the distinct delivery columns (representations), first-seen", () => {
    const reps = representationsFromStates([
      state({
        delivery_column_name: "agrupp",
        value_set_version_label: "5-års intervall",
        value_set: [{ code: "1", label: "a" }] as never,
      }),
      state({
        delivery_column_name: "agrupp2",
        value_set_version_label: "10-års intervall",
        value_set: null,
      }),
      // a second state on the same column collapses to the first.
      state({ delivery_column_name: "agrupp", value_set_version_label: "x" }),
    ]);
    expect(reps.map((r) => r.column)).toEqual(["agrupp", "agrupp2"]);
    expect(reps[0]).toEqual({
      column: "agrupp",
      label: "5-års intervall",
      codeCount: 1,
    });
    expect(reps[1].codeCount).toBeNull();
  });

  it("is empty / single when there is no real choice", () => {
    expect(representationsFromStates([])).toEqual([]);
    expect(
      representationsFromStates([state({ delivery_column_name: "kon" })]),
    ).toHaveLength(1);
    // null-column states are ignored (no handle to pin).
    expect(
      representationsFromStates([state({ delivery_column_name: null })]),
    ).toEqual([]);
  });

  it("does NOT treat a sequential column rename as a choice (no overlap)", () => {
    // KonOld 2010-2015, renamed KonNew 2016-9999 — never co-exist → drift, not a
    // representation choice; the chooser stays closed (length ≤ 1). Mirrors the
    // backend overlap gate.
    const reps = representationsFromStates([
      state({
        delivery_column_name: "KonOld",
        valid_from: "2010-01-01",
        valid_to: "2015-12-31",
      }),
      state({
        delivery_column_name: "KonNew",
        valid_from: "2016-01-01",
        valid_to: "9999-12-31",
      }),
    ]);
    expect(reps.length).toBeLessThanOrEqual(1);
  });

  it("treats overlapping distinct columns as co-existing representations", () => {
    const reps = representationsFromStates([
      state({
        delivery_column_name: "kon",
        valid_from: "2018-01-01",
        valid_to: "9999-12-31",
      }),
      state({
        delivery_column_name: "kon_detalj",
        valid_from: "2018-01-01",
        valid_to: "9999-12-31",
      }),
    ]);
    expect(reps.map((r) => r.column).sort()).toEqual(["kon", "kon_detalj"]);
  });
});
