import { describe, expect, it } from "vitest";
import type { CatalogNode, StatesResponse, VariableStateModel } from "./api";
import {
  axisNoun,
  bindingChildren,
  breadcrumbs,
  buildAddPlan,
  catalogHref,
  coverageFromStates,
  deriveType,
  foldText,
  formatDataType,
  formatStateWindow,
  fqidSegments,
  grainsFromStates,
  matchesFilter,
  memberCoverageUnion,
  narrowCatalogNode,
  nodeLabel,
  rankFilter,
  registerPrefixOf,
  representationsCollapse,
  representationsFromStates,
  stateChangeHints,
  stateKey,
  variantSeg,
  windowTitle,
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
    is_identifier: false,
    classification_slug: null,
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

describe("foldText", () => {
  it("strips diacritics and lowercases", () => {
    expect(foldText("Löne")).toBe("lone");
    expect(foldText("lön")).toBe("lon");
    expect(foldText("ÅÄÖ")).toBe("aao");
    expect(foldText("Kön")).toBe("kon");
  });
});

describe("matchesFilter", () => {
  it("empty / whitespace-only needle matches everything", () => {
    expect(matchesFilter("", "anything")).toBe(true);
    expect(matchesFilter("   ", "anything")).toBe(true);
  });

  it("is case-insensitive and diacritic-blind on both needle and haystack", () => {
    // "lon" matches "Löne…" (haystack has diacritic) and "lön" (needle-side).
    expect(matchesFilter("lon", "Löneutbetalning")).toBe(true);
    expect(matchesFilter("lön", "Loneutbetalning")).toBe(true);
    expect(matchesFilter("KON", "Kön")).toBe(true);
  });

  it("matches a substring against ANY of the haystacks (name OR slug/fqid)", () => {
    // Needle hits the FQID but not the display name.
    expect(matchesFilter("agi", "Annual income", "scb/lisa/agi_2019")).toBe(
      true,
    );
    // Needle hits neither.
    expect(matchesFilter("xyz", "Annual income", "scb/lisa/agi_2019")).toBe(
      false,
    );
  });

  it("skips null/undefined haystacks", () => {
    expect(matchesFilter("kon", null, undefined, "Kön")).toBe(true);
    expect(matchesFilter("kon", null, undefined)).toBe(false);
  });
});

describe("rankFilter", () => {
  // Alphabetical input (the picker's incoming order) with a few "kon" matches.
  const items = [
    { name: "Antal anställda enligt kontrolluppgift", fqid: "scb/lisa/anstku" },
    {
      name: "Disponibel inkomst per konsumtionsenhet",
      fqid: "scb/lisa/dispke",
    },
    { name: "Konsumtionsenheter, familj", fqid: "scb/lisa/kefam" },
    { name: "Kön", fqid: "scb/lisa/kon" },
    { name: "Yrke", fqid: "scb/lisa/ykon" },
  ];
  const keys = (i: { name: string; fqid: string }) => [i.fqid, i.name];

  it("ranks exact → prefix → other, keeping alphabetical order within a tier", () => {
    const out = rankFilter(items, "kon", keys).map((i) => i.name);
    // "Kön" (name folds to exact "kon") AND scb/lisa/kon (slug exact) → tier 0,
    // first. Then prefix matches ("Konsumtionsenheter…"). Then the rest, each
    // tier keeping the incoming alphabetical order.
    expect(out[0]).toBe("Kön");
    expect(out).toEqual([
      "Kön", // exact (slug `kon` + folded name `kon`)
      "Konsumtionsenheter, familj", // prefix
      "Antal anställda enligt kontrolluppgift", // other (substring)
      "Disponibel inkomst per konsumtionsenhet", // other
      "Yrke", // other — slug `ykon` contains "kon"
    ]);
  });

  it("empty needle returns every item, order unchanged", () => {
    expect(rankFilter(items, "", keys)).toEqual(items);
    expect(rankFilter(items, "  ", keys)).toEqual(items);
  });

  it("drops non-matches", () => {
    expect(rankFilter(items, "zzz", keys)).toEqual([]);
  });
});

describe("nodeLabel", () => {
  it("uses name when present, else falls back to fqid", () => {
    expect(nodeLabel(provider)).toBe("Statistics Sweden");
    expect(nodeLabel(register)).toBe("scb/lisa"); // name is null → fqid
    expect(nodeLabel(classification)).toBe("Education");
  });
});

describe("axisNoun", () => {
  it("pluralizes the group's single facet axis, else falls back to members", () => {
    // The real classification-umbrella case (#608) — must read "dimensions",
    // never a re-hardcoded "vintages".
    expect(axisNoun(["dimension"])).toBe("dimensions");
    expect(axisNoun(["vintage"])).toBe("vintages");
    expect(axisNoun([])).toBe("members"); // no axis → generic fallback
  });
});

describe("narrowCatalogNode", () => {
  it("keeps a `kind`-tagged node, drops a no-`kind` payload and null", () => {
    expect(narrowCatalogNode(provider)).toBe(provider);
    // A `?period` resolve returns a no-`kind` StatesResponse → not browsable.
    const states = { states: [] } as unknown as StatesResponse;
    expect(narrowCatalogNode(states)).toBeNull();
    expect(narrowCatalogNode(null)).toBeNull();
  });
});

describe("bindingChildren", () => {
  it("returns only the binding children of a register, in order", () => {
    const node = {
      kind: "register",
      fqid: "scb/lisa",
      name: null,
      purpose: null,
      children: [
        { kind: "binding", fqid: "scb/lisa/kon", name: "Sex" },
        { kind: "variants-ref" },
        { kind: "binding", fqid: "scb/lisa/alder", name: null },
      ],
    } as unknown as CatalogNode;
    expect(bindingChildren(node).map((c) => c.fqid)).toEqual([
      "scb/lisa/kon",
      "scb/lisa/alder",
    ]);
  });

  it("returns [] for a non-register node", () => {
    expect(bindingChildren(provider)).toEqual([]);
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
    // "date and time" must beat DATE despite the leading "datum" token.
    expect(deriveType(state({ data_type: "datum och klockslag" }))).toBe(
      "datetime",
    );
  });

  it("reg_meta is_identifier overrides the storage token (int → id, not numeric)", () => {
    expect(deriveType(state({ data_type: "int", is_identifier: true }))).toBe(
      "id",
    );
    expect(deriveType(state({ data_type: "int", is_identifier: false }))).toBe(
      "numeric",
    );
    // is_identifier also wins over the value_set → categorical check (it's
    // checked first).
    expect(
      deriveType(
        state({ data_type: "int", is_identifier: true, value_set_id: 5 }),
      ),
    ).toBe("id");
  });

  it("unrecognized / empty storage token → opaque (user picks)", () => {
    expect(deriveType(state({ data_type: "alfanumerisk" }))).toBe("opaque");
    expect(deriveType(state({ data_type: "Sträng (text)" }))).toBe("opaque");
    expect(deriveType(state({ data_type: "" }))).toBe("opaque");
    expect(deriveType(state({ data_type: "<undefined>" }))).toBe("opaque");
  });
});

describe("formatDataType", () => {
  it("drops a meaningless length (the bigint(0) artifact)", () => {
    expect(formatDataType("bigint", "0")).toBe("bigint");
    expect(formatDataType("Heltal", "0")).toBe("Heltal");
    expect(formatDataType("char", "")).toBe("char");
    expect(formatDataType("char", null)).toBe("char");
    expect(formatDataType("char", undefined)).toBe("char");
    // SQL Server's varchar(MAX) sentinel — same meaningless-parenthetical class.
    expect(formatDataType("nvarchar", "-1")).toBe("nvarchar");
  });

  it("keeps a meaningful non-zero length", () => {
    expect(formatDataType("char", "25")).toBe("char(25)");
    expect(formatDataType("alfanumerisk", "4")).toBe("alfanumerisk(4)");
    expect(formatDataType("Decimaltal", "4")).toBe("Decimaltal(4)");
  });

  it("drops a non-numeric length rather than printing garbage", () => {
    expect(formatDataType("char", "n/a")).toBe("char");
  });

  it("returns empty when there is no data_type", () => {
    expect(formatDataType(null, "4")).toBe("");
    expect(formatDataType("", "4")).toBe("");
    expect(formatDataType("  ", "4")).toBe("");
  });
});

describe("representationsFromStates", () => {
  it("returns the distinct delivery columns (representations), latest-era first", () => {
    const reps = representationsFromStates([
      state({
        delivery_column_name: "agrupp",
        valid_from: "2000-01-01",
        valid_to: "2010-12-31",
        value_set_version_label: "5-års intervall",
        value_set: [{ code: "1", label: "a" }] as never,
        classification_slug: "lkf2007",
      }),
      state({
        delivery_column_name: "agrupp2",
        valid_from: "2005-01-01",
        valid_to: "2015-12-31",
        value_set_version_label: "10-års intervall",
        value_set: null,
      }),
      // a second state on the same column collapses to the first — the rep's
      // classificationSlug stays the representative's (here null on the later
      // state, so a regression would surface as a flip to null).
      state({ delivery_column_name: "agrupp", value_set_version_label: "x" }),
    ]);
    // agrupp2 (valid_to 2015) outranks agrupp (2010) → latest-era first.
    expect(reps.map((r) => r.column)).toEqual(["agrupp2", "agrupp"]);
    // label / codeCount / classificationSlug are carried from the representative.
    expect(reps[1]).toMatchObject({
      column: "agrupp",
      label: "5-års intervall",
      codeCount: 1,
      classificationSlug: "lkf2007",
      validTo: "2010-12-31",
    });
    // codingKey = version label + sorted "code=label" pairs (content hash).
    expect(reps[1].codingKey).toContain("1=a");
    expect(reps[0].codeCount).toBeNull();
    // agrupp2's representative state is code-less → null classification slug.
    expect(reps[0].classificationSlug).toBeNull();
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
        classification_slug: "lkf2007",
      }),
      state({
        delivery_column_name: "kon_detalj",
        valid_from: "2018-01-01",
        valid_to: "9999-12-31",
        classification_slug: "lkf2016",
      }),
    ]);
    expect(reps.map((r) => r.column).sort()).toEqual(["kon", "kon_detalj"]);
    // Each co-existing representation keeps its OWN classification slug (the
    // crosswalk case — per-column fidelity, not a shared binding-level value).
    expect(reps.find((r) => r.column === "kon")?.classificationSlug).toBe(
      "lkf2007",
    );
    expect(
      reps.find((r) => r.column === "kon_detalj")?.classificationSlug,
    ).toBe("lkf2016");
  });

  it("ranks an open-ended column ahead of a dated one (latest-era primary)", () => {
    // UT0290 1980–1987 vs UT0280 1982–1983 — both overlap (coexist). The wider,
    // later-ending UT0290 leads. Then an open-ended (9999) column would beat both.
    const reps = representationsFromStates([
      state({
        delivery_column_name: "UT0280",
        valid_from: "1982-01-01",
        valid_to: "1983-12-31",
      }),
      state({
        delivery_column_name: "UT0290",
        valid_from: "1980-01-01",
        valid_to: "1987-12-31",
      }),
      state({
        delivery_column_name: "UT_open",
        valid_from: "1981-01-01",
        valid_to: "9999-12-31",
      }),
    ]);
    expect(reps.map((r) => r.column)).toEqual(["UT_open", "UT0290", "UT0280"]);
  });

  it("ranks by a column's LATEST era (max valid_to over its states)", () => {
    // colA has an early state but a later one extends it past colB → colA leads,
    // even though colB's first-seen state ends later than colA's first-seen state.
    const reps = representationsFromStates([
      state({
        delivery_column_name: "colA",
        valid_from: "1990-01-01",
        valid_to: "1995-12-31",
      }),
      state({
        delivery_column_name: "colB",
        valid_from: "1992-01-01",
        valid_to: "2000-12-31",
      }),
      state({
        delivery_column_name: "colA",
        valid_from: "2001-01-01",
        valid_to: "2010-12-31",
      }),
    ]);
    expect(reps[0].column).toBe("colA");
    expect(reps[0].validTo).toBe("2010-12-31");
  });
});

describe("representationsCollapse", () => {
  const coding = (
    column: string,
    label: string,
    value_set: { code: string; label: string }[] | null,
  ) =>
    state({
      delivery_column_name: column,
      valid_from: "1980-01-01",
      valid_to: "1987-12-31",
      value_set_version_label: label,
      value_set: value_set as never,
    });

  it("collapses coding-identical coexisting columns (UT0290/UT0280)", () => {
    // Same value-set content + label, two columns → collapse to primary + reveal.
    const reps = representationsFromStates([
      coding("UT0290", "Ja nej 1", [{ code: "1", label: "Ja" }]),
      coding("UT0280", "Ja nej 1", [{ code: "1", label: "Ja" }]),
    ]);
    expect(reps).toHaveLength(2);
    expect(representationsCollapse(reps)).toBe(true);
  });

  it("does NOT collapse genuinely distinct codings (SSYK 3/4/5-digit)", () => {
    const reps = representationsFromStates([
      coding("ssyk3", "3-siffer", [{ code: "1", label: "a" }]),
      coding("ssyk5", "5-siffer", [
        { code: "1", label: "a" },
        { code: "11", label: "b" },
      ]),
    ]);
    expect(representationsCollapse(reps)).toBe(false);
  });

  it("collapses two code-less columns sharing a version label", () => {
    const reps = representationsFromStates([
      coding("col_a", "identifierare", null),
      coding("col_b", "identifierare", null),
    ]);
    expect(representationsCollapse(reps)).toBe(true);
  });

  it("does NOT collapse code-less columns with different labels", () => {
    const reps = representationsFromStates([
      coding("col_a", "heltal", null),
      coding("col_b", "datum", null),
    ]);
    expect(representationsCollapse(reps)).toBe(false);
  });

  it("is true for 0/1-length lists (no choice to make)", () => {
    expect(representationsCollapse([])).toBe(true);
    expect(
      representationsCollapse(
        representationsFromStates([state({ delivery_column_name: "kon" })]),
      ),
    ).toBe(true);
  });
});

// ── Concept-group folding (#303) ─────────────────────────────────────────────

import type { ConceptGroup } from "./api";
import {
  axisValues,
  countFoldedMembers,
  foldGroupedRows,
  groupFilterKeys,
  groupMatchesFilter,
  memberAt,
} from "./catalog";

function group(over: Partial<ConceptGroup>): ConceptGroup {
  return {
    key: "ink",
    label: "Inkomst",
    source: "token",
    axes: ["month"],
    members: [
      {
        fqid: "scb/lisa/inkjan",
        name: "Inkomst i januari",
        facets: [{ axis: "month", value: "01", label: "januari" }],
      },
      {
        fqid: "scb/lisa/inkfeb",
        name: "Inkomst i februari",
        facets: [{ axis: "month", value: "02", label: "februari" }],
      },
    ],
    ...over,
  };
}

describe("foldGroupedRows", () => {
  const bindings = [
    { fqid: "scb/lisa/alder", name: "Ålder" },
    { fqid: "scb/lisa/inkfeb", name: "Inkomst i februari" },
    { fqid: "scb/lisa/inkjan", name: "Inkomst i januari" },
    { fqid: "scb/lisa/kon", name: "Kön" },
  ];

  it("folds grouped members under one group row, keeps ungrouped leaves", () => {
    const rows = foldGroupedRows(bindings, [group({})]);
    expect(
      rows.map((r) => (r.kind === "group" ? `g:${r.group.key}` : r.item.fqid)),
    ).toEqual(["scb/lisa/alder", "g:ink", "scb/lisa/kon"]);
  });

  it("is the identity (leaf rows in slug order) without groups", () => {
    const rows = foldGroupedRows(bindings, []);
    expect(rows.every((r) => r.kind === "leaf")).toBe(true);
    expect(rows).toHaveLength(4);
  });
});

describe("groupMatchesFilter", () => {
  it("matches on the group label/key", () => {
    expect(groupMatchesFilter("inkomst", group({}))).toBe(true);
    expect(groupMatchesFilter("ink", group({}))).toBe(true);
  });

  it("matches on a member name/FQID (folded, diacritic-blind)", () => {
    expect(groupMatchesFilter("februari", group({}))).toBe(true);
    expect(groupMatchesFilter("inkjan", group({}))).toBe(true);
    expect(groupMatchesFilter("nomatch", group({}))).toBe(false);
  });
});

describe("groupFilterKeys", () => {
  it("carries the label/key plus every member's name and FQID (#322)", () => {
    expect(groupFilterKeys(group({}))).toEqual([
      "Inkomst",
      "ink",
      "Inkomst i januari",
      "scb/lisa/inkjan",
      "Inkomst i februari",
      "scb/lisa/inkfeb",
    ]);
  });
});

describe("axisValues / memberAt", () => {
  const matrix = group({
    axes: ["month", "rank"],
    members: [
      {
        fqid: "scb/lisa/agi1inkjan",
        name: null,
        facets: [
          { axis: "month", value: "01", label: "januari" },
          { axis: "rank", value: "1", label: "största" },
        ],
      },
      {
        fqid: "scb/lisa/agi2inkjan",
        name: null,
        facets: [
          { axis: "month", value: "01", label: "januari" },
          { axis: "rank", value: "2", label: "näst största" },
        ],
      },
      {
        fqid: "scb/lisa/agi1inkfeb",
        name: null,
        facets: [
          { axis: "month", value: "02", label: "februari" },
          { axis: "rank", value: "1", label: "största" },
        ],
      },
    ],
  });

  it("collects distinct value-sorted axis values", () => {
    expect(axisValues(matrix, "month")).toEqual([
      { value: "01", label: "januari" },
      { value: "02", label: "februari" },
    ]);
    expect(axisValues(matrix, "rank")).toEqual([
      { value: "1", label: "största" },
      { value: "2", label: "näst största" },
    ]);
  });

  it("finds the member at a coordinate; empty cells are undefined", () => {
    expect(
      memberAt(matrix, [
        { axis: "month", value: "01" },
        { axis: "rank", value: "2" },
      ])?.fqid,
    ).toBe("scb/lisa/agi2inkjan");
    // partial family: feb has no rank-2 member
    expect(
      memberAt(matrix, [
        { axis: "month", value: "02" },
        { axis: "rank", value: "2" },
      ]),
    ).toBeUndefined();
  });
});

describe("foldGroupedRows order + countFoldedMembers", () => {
  // (imported above with the other #303 helpers)
  it("preserves the incoming item order (no re-sort)", () => {
    // Deliberately NON-alphabetical: classification-root children arrive
    // short_name-ordered; folding must not silently reorder them by slug.
    const items = [
      { fqid: "class/ssyk2012" },
      { fqid: "class/atc" },
      { fqid: "class/lkf1980" },
      { fqid: "class/lkf2020" },
      { fqid: "class/drg" },
    ];
    const lkf = group({
      key: "lkf",
      label: "LKF",
      axes: ["vintage"],
      members: [
        { fqid: "class/lkf1980", name: null, facets: [] },
        { fqid: "class/lkf2020", name: null, facets: [] },
      ],
    });
    const rows = foldGroupedRows(items, [lkf]);
    expect(
      rows.map((r) => (r.kind === "group" ? `g:${r.group.key}` : r.item.fqid)),
    ).toEqual(["class/ssyk2012", "class/atc", "g:lkf", "class/drg"]);
  });

  it("appends a group with no member present in items", () => {
    const rows = foldGroupedRows([{ fqid: "scb/lisa/kon" }], [group({})]);
    expect(
      rows.map((r) => (r.kind === "group" ? `g:${r.group.key}` : r.item.fqid)),
    ).toEqual(["scb/lisa/kon", "g:ink"]);
  });

  it("counts folded members in item units", () => {
    const rows = foldGroupedRows(
      [
        { fqid: "scb/lisa/kon" },
        { fqid: "scb/lisa/inkjan" },
        { fqid: "scb/lisa/inkfeb" },
      ],
      [group({})],
    );
    expect(countFoldedMembers(rows)).toBe(3); // 1 leaf + 2 grouped members
  });
});

describe("foldGroupedRows stale-cache tolerance", () => {
  it("degrades to flat leaves when groups is missing (stale edge-cached payload)", () => {
    const rows = foldGroupedRows(
      [{ fqid: "scb/lisa/kon" }, { fqid: "scb/lisa/alder" }],
      undefined,
    );
    expect(rows).toEqual([
      { kind: "leaf", item: { fqid: "scb/lisa/kon" } },
      { kind: "leaf", item: { fqid: "scb/lisa/alder" } },
    ]);
  });
});

describe("buildAddPlan (#306 one-click add)", () => {
  // Two-variant succession shaped like LISA adeldag: "16plus" 1992–2009,
  // "15plus" 2010–open. Distinct columns per era (a sequential rename, NOT a
  // representation choice — representationsFromStates treats non-overlapping
  // columns as drift).
  const succession = [
    state({
      state_id: 1,
      variant: "individer-16plus",
      valid_from: "1992-01-01",
      valid_to: "2009-12-31",
      delivery_column_name: "ADelDag",
    }),
    state({
      state_id: 2,
      variant: "individer-15plus",
      valid_from: "2010-01-01",
      valid_to: "9999-12-31",
      delivery_column_name: "ADelDag",
    }),
  ];

  it("no states → an empty segments plan", () => {
    expect(buildAddPlan([], "2018")).toEqual({
      kind: "segments",
      segments: [],
    });
  });

  it("a single variant → one segment with the period verbatim", () => {
    const plan = buildAddPlan(
      [
        state({
          variant: "v1",
          valid_from: "2010-01-01",
          valid_to: "2020-12-31",
        }),
      ],
      "2012..2014",
    );
    expect(plan.kind).toBe("segments");
    if (plan.kind === "segments") {
      expect(plan.segments).toHaveLength(1);
      expect(plan.segments[0].variant).toBe("v1");
      expect(plan.segments[0].period).toBe("2012..2014");
      expect(plan.segments[0].needsRepChoice).toBe(false);
      expect(plan.segments[0].representation).toBeNull();
    }
  });

  it("a range spanning a variant succession auto-splits, clipping each segment", () => {
    const plan = buildAddPlan(succession, "1992..2023");
    expect(plan.kind).toBe("segments");
    if (plan.kind === "segments") {
      expect(plan.segments.map((s) => [s.variant, s.period])).toEqual([
        ["individer-16plus", "1992..2009"],
        ["individer-15plus", "2010..2023"],
      ]);
    }
  });

  it("the user's endpoint tokens survive verbatim at the range edges", () => {
    const plan = buildAddPlan(succession, "VT1992..HT2023");
    expect(plan.kind).toBe("segments");
    if (plan.kind === "segments") {
      expect(plan.segments.map((s) => s.period)).toEqual([
        "VT1992..2009",
        "2010..HT2023",
      ]);
    }
  });

  it("a mid-year succession boundary stays an exact date token", () => {
    const midYear = [
      state({
        state_id: 1,
        variant: "a",
        valid_from: "1992-01-01",
        valid_to: "2009-06-30",
      }),
      state({
        state_id: 2,
        variant: "b",
        valid_from: "2009-07-01",
        valid_to: "9999-12-31",
      }),
    ];
    const plan = buildAddPlan(midYear, "1992..2023");
    expect(plan.kind).toBe("segments");
    if (plan.kind === "segments") {
      expect(plan.segments.map((s) => s.period)).toEqual([
        "1992..2009-06-30",
        "2009-07-01..2023",
      ]);
    }
  });

  it("a single-year clip collapses to one token (no degenerate range)", () => {
    const plan = buildAddPlan(succession, "2009..2010");
    expect(plan.kind).toBe("segments");
    if (plan.kind === "segments") {
      expect(plan.segments.map((s) => s.period)).toEqual(["2009", "2010"]);
    }
  });

  it("co-existing variants inside the range → choose-variant", () => {
    const coexisting = [
      state({
        state_id: 1,
        variant: "individer",
        valid_from: "1992-01-01",
        valid_to: "9999-12-31",
      }),
      state({
        state_id: 2,
        variant: "arbetsstallen",
        valid_from: "1992-01-01",
        valid_to: "9999-12-31",
      }),
    ];
    const plan = buildAddPlan(coexisting, "2010..2020");
    expect(plan.kind).toBe("choose-variant");
    if (plan.kind === "choose-variant") {
      expect(plan.options.map((o) => o.variant)).toEqual([
        "arbetsstallen",
        "individer",
      ]);
    }
  });

  it("≥2 variants with a point period (or none) → choose-variant", () => {
    // A point period can't span a succession; ≥2 remaining variants co-exist.
    expect(buildAddPlan(succession, "2009").kind).toBe("choose-variant");
    expect(buildAddPlan(succession, null).kind).toBe("choose-variant");
    expect(buildAddPlan(succession, "_default").kind).toBe("choose-variant");
  });

  it("genuinely distinct co-existing codings flag a rep choice, primary preselected", () => {
    const multiRep = [
      state({
        state_id: 1,
        variant: "v1",
        valid_from: "2010-01-01",
        valid_to: "2020-12-31",
        delivery_column_name: "Ssyk3",
        value_set: [{ code: "1", label: "a" }],
        value_set_version_label: "3-digit",
      }),
      state({
        state_id: 2,
        variant: "v1",
        valid_from: "2010-01-01",
        valid_to: "2023-12-31",
        delivery_column_name: "Ssyk4",
        value_set: [{ code: "11", label: "b" }],
        value_set_version_label: "4-digit",
      }),
    ];
    const plan = buildAddPlan(multiRep, "2018");
    expect(plan.kind).toBe("segments");
    if (plan.kind === "segments") {
      expect(plan.segments[0].needsRepChoice).toBe(true);
      // Primary = latest-era (Ssyk4, valid_to 2023) per #266.
      expect(plan.segments[0].representation).toBe("Ssyk4");
      expect(plan.segments[0].reps.map((r) => r.column)).toEqual([
        "Ssyk4",
        "Ssyk3",
      ]);
    }
  });

  it("coding-identical parallel columns auto-pick the primary (no prompt)", () => {
    const identical = [
      state({
        state_id: 1,
        variant: "v1",
        valid_from: "1980-01-01",
        valid_to: "1987-12-31",
        delivery_column_name: "UT0290",
        value_set: [{ code: "1", label: "Ja" }],
        value_set_version_label: "Ja nej 1",
      }),
      state({
        state_id: 2,
        variant: "v1",
        valid_from: "1982-01-01",
        valid_to: "1983-12-31",
        delivery_column_name: "UT0280",
        value_set: [{ code: "1", label: "Ja" }],
        value_set_version_label: "Ja nej 1",
      }),
    ];
    const plan = buildAddPlan(identical, "1982");
    expect(plan.kind).toBe("segments");
    if (plan.kind === "segments") {
      expect(plan.segments[0].needsRepChoice).toBe(false);
      expect(plan.segments[0].representation).toBe("UT0290");
    }
  });
});

describe("formatStateWindow (#309 sentinel hiding + #321 period tokens)", () => {
  it("an open-ended state reads 'since <from>' (sentinel hidden, year-collapsed)", () => {
    expect(
      formatStateWindow(
        state({ valid_from: "2016-01-01", valid_to: "9999-12-31" }),
      ),
    ).toBe("since 2016");
    expect(
      formatStateWindow(
        state({ valid_from: "2016-07-01", valid_to: "9999-12-31" }),
      ),
    ).toBe("since 2016-07-01");
  });

  it("prefers the backend's single coarsest-exact token", () => {
    expect(
      formatStateWindow(
        state({
          valid_from: "2009-01-01",
          valid_to: "2009-06-30",
          period_token: "VT2009",
        }),
      ),
    ).toBe("VT2009");
    expect(
      formatStateWindow(
        state({
          valid_from: "2018-01-01",
          valid_to: "2018-12-31",
          period_token: "2018",
        }),
      ),
    ).toBe("2018");
  });

  it("falls back to a year-collapsed range for multi-year windows and token-less payloads", () => {
    // A multi-year window's token is the explicit lo..hi range → fall back.
    expect(
      formatStateWindow(
        state({
          valid_from: "1992-01-01",
          valid_to: "2009-12-31",
          period_token: "1992-01-01..2009-12-31",
        }),
      ),
    ).toBe("1992 – 2009");
    // A stale edge-cached payload missing the field entirely (#317 rule).
    expect(
      formatStateWindow(
        state({ valid_from: "1992-01-01", valid_to: "2009-12-31" }),
      ),
    ).toBe("1992 – 2009");
    // Mid-year bounds stay exact dates.
    expect(
      formatStateWindow(
        state({ valid_from: "2009-07-01", valid_to: "2010-06-30" }),
      ),
    ).toBe("2009-07-01 – 2010-06-30");
    // A PARTIALLY year-aligned window keeps both exact dates — a one-sided
    // collapse would read backwards ("2009-07-01 – 2009") for a stale-cached
    // HT window.
    expect(
      formatStateWindow(
        state({ valid_from: "2009-07-01", valid_to: "2009-12-31" }),
      ),
    ).toBe("2009-07-01 – 2009-12-31");
    expect(
      formatStateWindow(
        state({ valid_from: "2009-01-01", valid_to: "2009-06-30" }),
      ),
    ).toBe("2009-01-01 – 2009-06-30");
  });
});

describe("stateChangeHints (#309 what-differs)", () => {
  it("flags a data-type-only change between adjacent same-variant states (int → bigint)", () => {
    const s1 = state({
      state_id: 1,
      variant: "v1",
      valid_from: "2010-01-01",
      valid_to: "2015-12-31",
      data_type: "int",
    });
    const s2 = state({
      state_id: 2,
      variant: "v1",
      valid_from: "2016-01-01",
      valid_to: "2023-12-31",
      data_type: "bigint",
    });
    const hints = stateChangeHints([s1, s2]);
    expect(hints.get(stateKey(s2))).toEqual(["type int → bigint"]);
    expect(hints.has(stateKey(s1))).toBe(false);
  });

  it("flags a column rename and a value-set content change (same label → 'value set changed')", () => {
    const s1 = state({
      state_id: 1,
      variant: "v1",
      valid_from: "2010-01-01",
      valid_to: "2015-12-31",
      delivery_column_name: "Old",
      value_set_id: 10,
      value_set_version_label: "v",
    });
    const s2 = state({
      state_id: 2,
      variant: "v1",
      valid_from: "2016-01-01",
      valid_to: "2023-12-31",
      delivery_column_name: "New",
      value_set_id: 11, // content key differs, label does NOT
      value_set_version_label: "v",
    });
    const hints = stateChangeHints([s1, s2]);
    expect(hints.get(stateKey(s2))).toEqual([
      "column Old → New",
      "value set changed",
    ]);
  });

  it("names the labels when the value-set version label changes too", () => {
    const s1 = state({
      state_id: 1,
      variant: "v1",
      valid_from: "2010-01-01",
      valid_to: "2015-12-31",
      value_set_id: 10,
      value_set_version_label: "SSYK 96",
    });
    const s2 = state({
      state_id: 2,
      variant: "v1",
      valid_from: "2016-01-01",
      valid_to: "2023-12-31",
      value_set_id: 11,
      value_set_version_label: "SSYK 2012",
    });
    const hints = stateChangeHints([s1, s2]);
    expect(hints.get(stateKey(s2))).toEqual(["value set SSYK 96 → SSYK 2012"]);
  });

  it("never diffs across the same-state_id windows of a merged monthly family (#319/#384)", () => {
    // ONE annual state (state_id 10) expanded into 3 per-month windows: SAME
    // variant + state_id, distinct per-month delivery columns and
    // non-overlapping consecutive months. The windows are 3 representations of
    // a single claim, not a column succession — no spurious
    // "column LonFinkJan → LonFinkFeb" hint.
    const hints = stateChangeHints([
      state({
        state_id: 10,
        variant: "v1",
        valid_from: "2020-01-01",
        valid_to: "2020-01-31",
        delivery_column_name: "LonFinkJan",
      }),
      state({
        state_id: 10,
        variant: "v1",
        valid_from: "2020-02-01",
        valid_to: "2020-02-29",
        delivery_column_name: "LonFinkFeb",
      }),
      state({
        state_id: 10,
        variant: "v1",
        valid_from: "2020-03-01",
        valid_to: "2020-03-31",
        delivery_column_name: "LonFinkMar",
      }),
    ]);
    expect(hints.size).toBe(0);
  });

  it("hints a genuine transition INTO a merged family without collapsing onto its sibling windows (#384)", () => {
    // A prior annual state (state_id 9, earlier non-overlapping window, distinct
    // column) succeeded by a merged monthly family (state_id 10, ≥2 windows).
    const prior = state({
      state_id: 9,
      variant: "v1",
      valid_from: "2019-01-01",
      valid_to: "2019-12-31",
      delivery_column_name: "LonFinkArs",
    });
    const jan = state({
      state_id: 10,
      variant: "v1",
      valid_from: "2020-01-01",
      valid_to: "2020-01-31",
      delivery_column_name: "LonFinkJan",
    });
    const feb = state({
      state_id: 10,
      variant: "v1",
      valid_from: "2020-02-01",
      valid_to: "2020-02-29",
      delivery_column_name: "LonFinkFeb",
    });
    const hints = stateChangeHints([prior, jan, feb]);
    // Exactly ONE hint — the prior → first-family-window transition. Keyed by
    // the compound key, so it lands on the first window only…
    expect(hints.size).toBe(1);
    expect(hints.get(stateKey(jan))).toEqual([
      "column LonFinkArs → LonFinkJan",
    ]);
    // …and the sibling window neither collapses the hint onto itself nor gets a
    // spurious cross-window hint of its own.
    expect(hints.get(stateKey(feb))).toBeUndefined();
  });

  it("never diffs OVERLAPPING same-variant states (parallel alternatives, not a transition)", () => {
    // Two co-delivered vintages at the same window (Codex P2 on #335): a
    // chronological "changed" hint would be misleading — these co-exist.
    const hints = stateChangeHints([
      state({
        state_id: 1,
        variant: "v1",
        valid_from: "2010-01-01",
        valid_to: "2020-12-31",
        delivery_column_name: "Ssyk3",
        value_set_id: 1,
      }),
      state({
        state_id: 2,
        variant: "v1",
        valid_from: "2010-01-01",
        valid_to: "2023-12-31",
        delivery_column_name: "Ssyk4",
        value_set_id: 2,
      }),
    ]);
    expect(hints.size).toBe(0);
  });

  it("never hints across variants and stays silent for identical shapes", () => {
    const hints = stateChangeHints([
      state({
        state_id: 1,
        variant: "a",
        valid_from: "1992-01-01",
        valid_to: "2009-12-31",
        data_type: "int",
      }),
      state({
        state_id: 2,
        variant: "b", // a variant change is visible on the row, not a hint
        valid_from: "2010-01-01",
        valid_to: "2015-12-31",
        data_type: "bigint",
      }),
      state({
        state_id: 3,
        variant: "b", // identical shape to state 2 → no hint
        valid_from: "2016-01-01",
        valid_to: "2023-12-31",
        data_type: "bigint",
      }),
    ]);
    expect(hints.size).toBe(0);
  });
});

describe("windowTitle (#309 sentinel-free tooltips)", () => {
  it("renders exact dates; the sentinel reads open-ended", () => {
    expect(windowTitle("2010-01-01", "2015-12-31")).toBe(
      "2010-01-01 – 2015-12-31",
    );
    expect(windowTitle("2016-01-01", "9999-12-31")).toBe(
      "2016-01-01 – open-ended",
    );
  });
});

describe("grainsFromStates (#308 grain pre-narrowing)", () => {
  it("always offers year; finer grains come from the #321 tokens, coarse → fine", () => {
    expect(
      grainsFromStates([
        state({ state_id: 1, period_token: "VT2006" }),
        state({ state_id: 2, period_token: "2007-08" }),
        state({ state_id: 3, period_token: "2008" }),
      ]),
    ).toEqual(["year", "term", "month"]);
  });

  it("degrades to year-only on token-less payloads (stale edge cache) and lo..hi tokens", () => {
    expect(grainsFromStates([state({ state_id: 1 })])).toEqual(["year"]);
    expect(
      grainsFromStates([
        state({ state_id: 1, period_token: "1992-01-01..2009-12-31" }),
      ]),
    ).toEqual(["year"]);
  });
});

describe("coverageFromStates (#615 availability span)", () => {
  it("spans min(valid_from) to max(valid_to) as year ints", () => {
    expect(
      coverageFromStates([
        state({
          state_id: 1,
          valid_from: "2000-01-01",
          valid_to: "2008-12-31",
        }),
        state({
          state_id: 2,
          valid_from: "1995-01-01",
          valid_to: "2010-06-30",
        }),
      ]),
    ).toEqual({ from: 1995, to: 2010 });
  });

  it("the open-ended sentinel leaves the END unbounded (null), start preserved", () => {
    // `9999-12-31` = "still delivered" → `to: null`; the picker projects the open
    // end to the slider's vintage ceiling, never a literal 9999 track.
    expect(
      coverageFromStates([
        state({
          state_id: 1,
          valid_from: "2005-01-01",
          valid_to: "9999-12-31",
        }),
      ]),
    ).toEqual({ from: 2005, to: null });
  });

  it("the yearless floor (0001) leaves the START unbounded but PRESERVES a finite end", () => {
    // The round-1 regression: `0001-01-01..2008-12-31` (unknown start, KNOWN end)
    // must keep `to: 2008` (only the start is unbounded), NOT collapse the whole
    // span to null — else a 2010–2015 selection loses its "Not delivered after
    // 2008" warning (Codex P2 round 2, Fix A).
    expect(
      coverageFromStates([
        state({
          state_id: 1,
          valid_from: "0001-01-01",
          valid_to: "2008-12-31",
        }),
      ]),
    ).toEqual({ from: null, to: 2008 });
  });

  it("null on an empty / bound-less state set", () => {
    expect(coverageFromStates([])).toBeNull();
    expect(
      coverageFromStates([
        state({ state_id: 1, valid_from: "", valid_to: "" }),
      ]),
    ).toBeNull();
  });

  it("a wholly-sentinel state (0001..9999) is unbounded on BOTH sides → null", () => {
    // Both bounds are sentinels, so coverage is fully unknown — no finite side to
    // draw or gap against (NOT { from: 1, … }, which would let the slider emit
    // out-of-grammar wires like `1..2026`).
    expect(
      coverageFromStates([
        state({
          state_id: 1,
          valid_from: "0001-01-01",
          valid_to: "9999-12-31",
        }),
      ]),
    ).toBeNull();
  });

  it("a 0001-floor state alongside a real-year state → finite start from the real year", () => {
    expect(
      coverageFromStates([
        state({
          state_id: 1,
          valid_from: "0001-01-01",
          valid_to: "2008-12-31",
        }),
        state({
          state_id: 2,
          valid_from: "2002-01-01",
          valid_to: "2010-12-31",
        }),
      ]),
    ).toEqual({ from: 2002, to: 2010 });
  });
});

describe("memberCoverageUnion (#638 PR2a group availability span)", () => {
  it("unions finite member spans to year ints", () => {
    expect(
      memberCoverageUnion([
        {
          coverage_from: "2000-01-01",
          coverage_to: "2008-12-31",
          open_ended: false,
        },
        {
          coverage_from: "1995-01-01",
          coverage_to: "2010-06-30",
          open_ended: false,
        },
      ]),
    ).toEqual({ from: 1995, to: 2010 });
  });

  it("an open-ended member unbounds the union END (null), start preserved", () => {
    expect(
      memberCoverageUnion([
        {
          coverage_from: "2000-01-01",
          coverage_to: "2008-12-31",
          open_ended: false,
        },
        { coverage_from: "2005-01-01", coverage_to: null, open_ended: true },
      ]),
    ).toEqual({ from: 2000, to: null });
  });

  it("a member with no finite end (null coverage_to) also unbounds the END", () => {
    expect(
      memberCoverageUnion([
        { coverage_from: "2000-01-01", coverage_to: null, open_ended: false },
      ]),
    ).toEqual({ from: 2000, to: null });
  });

  it("skips null / stateless members", () => {
    expect(
      memberCoverageUnion([
        null,
        undefined,
        {
          coverage_from: "2001-01-01",
          coverage_to: "2003-12-31",
          open_ended: false,
        },
      ]),
    ).toEqual({ from: 2001, to: 2003 });
  });

  it("null when no member contributes a finite bound and none is open-ended", () => {
    expect(memberCoverageUnion([])).toBeNull();
    expect(memberCoverageUnion([null, undefined])).toBeNull();
    expect(
      memberCoverageUnion([
        { coverage_from: null, coverage_to: null, open_ended: false },
      ]),
    ).toBeNull();
  });

  it("a stateless member ({null,null,false}) does NOT unbound the union END", () => {
    // The stateless payload carries no span — it must be skipped, NOT treated as
    // open-ended. Its null `coverage_to` would otherwise trip the open-ended
    // branch and unbound the whole union (`to: null`), drawing the union track
    // through the vintage even though every finite member ends earlier.
    expect(
      memberCoverageUnion([
        {
          coverage_from: "2005-01-01",
          coverage_to: "2010-12-31",
          open_ended: false,
        },
        { coverage_from: null, coverage_to: null, open_ended: false },
      ]),
    ).toEqual({ from: 2005, to: 2010 });
  });

  it("a yearless-floor start (0001-01-01) does NOT floor the union to year 1", () => {
    // The `0001-01-01` start sentinel means "start unknown", not year 1 — it must
    // be treated as no finite start (mirrors `coverageFromStates`), else the union
    // `from` floors to 1 and balloons the PeriodPicker slider track.
    expect(
      memberCoverageUnion([
        {
          coverage_from: "0001-01-01",
          coverage_to: "2008-12-31",
          open_ended: false,
        },
      ]),
    ).toEqual({ from: null, to: 2008 });
  });
});
