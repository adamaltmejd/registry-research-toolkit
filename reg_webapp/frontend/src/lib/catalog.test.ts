import { describe, expect, it } from "vitest";
import type { CatalogNode, StatesResponse, VariableStateModel } from "./api";
import {
  bindingChildren,
  breadcrumbs,
  catalogHref,
  deriveType,
  foldText,
  formatDataType,
  fqidSegments,
  matchesFilter,
  narrowCatalogNode,
  nodeLabel,
  rankFilter,
  registerPrefixOf,
  representationsCollapse,
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
