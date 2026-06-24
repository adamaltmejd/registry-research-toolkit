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
  distinctValueSets,
  foldText,
  formatDataType,
  formatStateWindow,
  formatWindow,
  fqidSegments,
  grainsFromStates,
  groupHref,
  humanizeClassificationSlug,
  leafSlug,
  matchesFilter,
  memberCoverageUnion,
  memberGroupLink,
  memberQualifier,
  narrowCatalogNode,
  nodeLabel,
  rankFilter,
  registerPrefixOf,
  representationsCollapse,
  representationsFromStates,
  variantSeg,
  windowTitle,
  YEARLESS_VALID_FROM,
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

  it("ranks a leaf-slug match above a purpose-blurb-only match (#674)", () => {
    // The register-browse case: `scb/rtb` matches the needle "rtb" by its leaf
    // slug; `scb/breg` matches ONLY via its purpose blurb. Without the leaf-slug
    // key both land at tier 2 (the provider prefix blocks a `scb/rtb` PREFIX
    // match) and `breg` wins alphabetically. With `leafSlug` as a key, `rtb` is a
    // tier-0 exact and outranks `breg` — while `breg` still appears (it matches).
    const registers = [
      {
        name: "Företagsregister", // breg, alphabetically first
        fqid: "scb/breg",
        purpose: "Registret över totalbefolkningen och dess struktur (rtb)",
      },
      {
        name: "Registret över totalbefolkningen",
        fqid: "scb/rtb",
        purpose: "Befolkningsdata",
      },
    ];
    const out = rankFilter(registers, "rtb", (r) => [
      leafSlug(r.fqid),
      r.name,
      r.fqid,
      r.purpose,
    ]).map((r) => r.fqid);
    expect(out).toEqual(["scb/rtb", "scb/breg"]);
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

describe("groupHref", () => {
  it("builds the /catalog/group/<provider>/<register>/<key> route from a 2-seg register fqid", () => {
    // #673: the register-arm group rows link to the group SUBJECT page (a fixed
    // register-only route), NOT the /catalog/<fqid> browse path.
    expect(groupHref("scb/lisa", "ink")).toBe("/catalog/group/scb/lisa/ink");
  });

  it("percent-encodes each segment the same way catalogHref does", () => {
    // A reserved/non-ASCII char in the key can't produce a malformed URL; the
    // `/` separators between the fixed segments survive (per-segment encoding).
    expect(groupHref("scb/lsön", "a/b")).toBe(
      "/catalog/group/scb/ls%C3%B6n/a%2Fb",
    );
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

describe("leafSlug", () => {
  it("returns the last segment of an FQID; a bare slug is itself", () => {
    expect(leafSlug("scb/rtb")).toBe("rtb");
    expect(leafSlug("scb/lisa/kon")).toBe("kon");
    expect(leafSlug("rtb")).toBe("rtb"); // no separator → the whole string
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

import type { BindingGroupRef, ConceptGroup } from "./api";
import {
  axisValues,
  countFoldedMembers,
  foldGroupedRows,
  groupFilterKeys,
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

describe("groupFilterKeys", () => {
  it("carries the label/key plus every member's name, FQID, and leaf slug (#322, #674)", () => {
    expect(groupFilterKeys(group({}))).toEqual([
      "Inkomst",
      "ink",
      "Inkomst i januari",
      "scb/lisa/inkjan",
      "inkjan",
      "Inkomst i februari",
      "scb/lisa/inkfeb",
      "inkfeb",
    ]);
  });

  it("ranks the folding group at exact/prefix tier on a member-slug needle (#674)", () => {
    // A hidden member's leaf slug ("inkjan") now ranks its folding group at
    // prefix tier (1) — tier 0/1 — rather than as an "other substring" (2),
    // consistent with how leaf rows rank by `leafSlug`. We rank the group row
    // against an unrelated decoy whose only match is a tier-2 substring.
    type Row = { id: string; group?: ConceptGroup };
    const rows: Row[] = [
      { id: "decoy" }, // matches "inkjan" only via the substring below
      { id: "ink-group", group: group({}) },
    ];
    const keysOf = (r: Row): (string | null | undefined)[] =>
      r.group ? groupFilterKeys(r.group) : [`zzz-inkjan-zzz`];
    const ranked = rankFilter(rows, "inkjan", keysOf);
    expect(ranked[0].id).toBe("ink-group");
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

describe("memberQualifier (#670)", () => {
  const naringsgren = group({
    key: "naringsgren",
    label: "Näringsgren, största förvärvskälla",
    axes: ["source", "edition"],
    members: [
      {
        fqid: "scb/lisa/agi1astsni2007g",
        name: "Näringsgren",
        facets: [
          { axis: "source", value: "agi", label: "AGI" },
          { axis: "edition", value: "sni2007", label: "2007 SNI edition" },
        ],
      },
      {
        fqid: "scb/lisa/ku1astsni2002g",
        name: "Näringsgren",
        facets: [{ axis: "source", value: "ku", label: "KU" }],
      },
    ],
  });

  it("joins a member's facet labels (multiple facets)", () => {
    expect(memberQualifier([naringsgren], "scb/lisa/agi1astsni2007g")).toEqual({
      text: "AGI · 2007 SNI edition",
      kind: "facets",
    });
  });

  it("handles a single facet", () => {
    expect(memberQualifier([naringsgren], "scb/lisa/ku1astsni2002g")).toEqual({
      text: "KU",
      kind: "facets",
    });
  });

  it("returns null for an UNGROUPED member (in no group, no canonical key)", () => {
    expect(memberQualifier([naringsgren], "scb/lisa/notamember")).toBeNull();
  });

  it("falls back to the member slug for a GROUPED facet-less member (edge group split sibling)", () => {
    // M10's exact case: an edge group (`axes: []`) whose members carry no
    // facets, so the slug is the only differentiator between the siblings.
    const facetless = group({
      key: "edge",
      axes: [],
      members: [
        { fqid: "scb/lisa/agi1astsni2007g", name: "Näringsgren", facets: [] },
        { fqid: "scb/lisa/ku1astsni", name: "Näringsgren", facets: [] },
      ],
    });
    expect(memberQualifier([facetless], "scb/lisa/agi1astsni2007g")).toEqual({
      text: "agi1astsni2007g",
      kind: "slug",
    });
    expect(memberQualifier([facetless], "scb/lisa/ku1astsni")).toEqual({
      text: "ku1astsni",
      kind: "slug",
    });
  });

  it("falls back to the slug when the member is grouped only via its canonical key", () => {
    // The member isn't found in the passed `groups` (e.g. a /dimensions skew),
    // but `node.group` (canonicalKey) marks it grouped → still distinguish it.
    expect(
      memberQualifier([], "scb/lisa/agi1astsni2007g", "naringsgren"),
    ).toEqual({ text: "agi1astsni2007g", kind: "slug" });
  });

  it("yields a safe slug result for a grouped member whose fqid has no 3rd segment", () => {
    // Defensive: a malformed/2-seg fqid on a grouped member (via the canonical
    // key) must NOT masquerade as a facet qualifier. `leafSlug` yields the last
    // segment (here "lisa"), and the result is the slug kind — a `<code>`
    // identifier, never a human facet label. The point is the DISCRIMINANT
    // (`kind: "slug"`), so the styling can't mis-render it.
    expect(memberQualifier([], "scb/lisa", "naringsgren")).toEqual({
      text: "lisa",
      kind: "slug",
    });
  });

  it("prefers the canonical group's facets when the member is in several groups", () => {
    // The same member appears in two groups; the canonical key selects which
    // group's facets lead.
    const other = group({
      key: "other",
      axes: ["level"],
      members: [
        {
          fqid: "scb/lisa/agi1astsni2007g",
          name: "Näringsgren",
          facets: [{ axis: "level", value: "5", label: "5-digit" }],
        },
      ],
    });
    // canonical = "naringsgren" → its facets win even though `other` is first.
    expect(
      memberQualifier(
        [other, naringsgren],
        "scb/lisa/agi1astsni2007g",
        "naringsgren",
      ),
    ).toEqual({ text: "AGI · 2007 SNI edition", kind: "facets" });
    // canonical = "other" → the level facet wins instead.
    expect(
      memberQualifier(
        [other, naringsgren],
        "scb/lisa/agi1astsni2007g",
        "other",
      ),
    ).toEqual({ text: "5-digit", kind: "facets" });
  });
});

describe("memberGroupLink (#670)", () => {
  const ref: BindingGroupRef = {
    provider: "scb",
    register: "lisa",
    key: "naringsgren",
  };
  const naringsgren = group({
    key: "naringsgren",
    label: "Näringsgren, största förvärvskälla",
    members: [{ fqid: "scb/lisa/agi1astsni2007g", name: "N", facets: [] }],
  });

  it("returns the matching group's label + the group-subject href", () => {
    expect(
      memberGroupLink([naringsgren], ref, "scb/lisa/agi1astsni2007g"),
    ).toEqual({
      label: "Näringsgren, största förvärvskälla",
      href: "/catalog/group/scb/lisa/naringsgren",
    });
  });

  it("returns null when the binding is ungrouped (no ref)", () => {
    expect(
      memberGroupLink([naringsgren], null, "scb/lisa/agi1astsni2007g"),
    ).toBeNull();
  });

  it("returns null when no fetched group matches (key + member miss)", () => {
    expect(memberGroupLink([], ref, "scb/lisa/agi1astsni2007g")).toBeNull();
  });

  it("falls back to the group containing the member when the key doesn't match", () => {
    // Defensive: a skew between node.group.key and /dimensions still links if the
    // member is found in some fetched group (uses the ref's href, that group's label).
    const skewed = group({
      key: "different-key",
      label: "Näringsgren (skewed)",
      members: [{ fqid: "scb/lisa/agi1astsni2007g", name: "N", facets: [] }],
    });
    expect(memberGroupLink([skewed], ref, "scb/lisa/agi1astsni2007g")).toEqual({
      label: "Näringsgren (skewed)",
      href: "/catalog/group/scb/lisa/naringsgren",
    });
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

describe("formatWindow open-start (#658)", () => {
  it("renders a one-sided 'until <year>' for a yearless start with a finite end", () => {
    // The mirror of the open-ended 'since <year>': an unknown start (the 0001
    // floor) with a known end shows only the end, year-collapsed when Dec-31-
    // aligned — never leaking the sentinel as "0001 – 2008".
    expect(formatWindow(YEARLESS_VALID_FROM, "2008-12-31")).toBe("until 2008");
    expect(formatWindow(YEARLESS_VALID_FROM, "2008-12-31")).not.toContain(
      "0001",
    );
  });

  it("keeps a mid-year end as an exact date token (valid grammar), like 'since'", () => {
    expect(formatWindow(YEARLESS_VALID_FROM, "2008-06-30")).toBe(
      "until 2008-06-30",
    );
  });

  it("a wholly-unbounded 0001..9999 window keeps 'since' (open-end wins)", () => {
    // Out of #658's finite-end scope, but pin the precedence: the open-ended form
    // is checked first, so this does NOT become "until 9999".
    expect(formatWindow(YEARLESS_VALID_FROM, "9999-12-31")).toBe("since 0001");
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

describe("humanizeClassificationSlug (#668)", () => {
  it("renders the clean <letters><year> vintage form as 'LETTERS year'", () => {
    expect(humanizeClassificationSlug("lkf2007")).toBe("LKF 2007");
    expect(humanizeClassificationSlug("lkf1980")).toBe("LKF 1980");
    expect(humanizeClassificationSlug("isced2011")).toBe("ISCED 2011");
  });

  it("falls back to the raw slug verbatim when it doesn't parse", () => {
    // Suffixed / hyphenated / non-vintage slugs keep their stable identifier.
    expect(humanizeClassificationSlug("sun-niva2000")).toBe("sun-niva2000");
    expect(humanizeClassificationSlug("icd-10-se")).toBe("icd-10-se");
    expect(humanizeClassificationSlug("atc")).toBe("atc");
    // A trailing-suffix vintage isn't the clean form → verbatim.
    expect(humanizeClassificationSlug("agi1astsni2007g")).toBe(
      "agi1astsni2007g",
    );
  });
});

describe("distinctValueSets (#668 — value-set-centric fold)", () => {
  // The kommun shape in miniature: states fanned across variants × vintages, many
  // sharing one `value_set_id` — and several `value_set_id`s sharing one
  // classification edition (the M13 two-level dedup).
  it("dedups NON-classification states by value_set_id, preserving first-seen order", () => {
    const states = [
      state({ value_set_id: 10, variant: "a", valid_from: "2000-01-01" }),
      state({ value_set_id: 20, variant: "a", valid_from: "2001-01-01" }),
      state({ value_set_id: 10, variant: "b", valid_from: "2000-01-01" }),
    ];
    const vs = distinctValueSets(states);
    expect(vs.map((v) => v.key)).toEqual(["id/10", "id/20"]);
  });

  it("collapses several value_set_ids that share one classification_slug into ONE entry (M13)", () => {
    // kommun's LKF editions: SCB ships ≥2 distinct `value_set_id`s per vintage
    // (lkf1980 ×2, lkf1995 ×3, …). They are the SAME classification edition, so
    // they MUST collapse to one "= LKF 1980" row — not a duplicate per id.
    const states = [
      state({
        value_set_id: 100,
        classification_slug: "lkf1980",
        variant: "doda",
        valid_from: "1980-01-01",
        valid_to: "1980-12-31",
      }),
      state({
        value_set_id: 101, // distinct id, SAME edition
        classification_slug: "lkf1980",
        variant: "fodda",
        valid_from: "1981-01-01",
        valid_to: "1981-12-31",
      }),
      state({
        value_set_id: 200,
        classification_slug: "lkf1995",
        variant: "doda",
        valid_from: "1995-01-01",
        valid_to: "1995-12-31",
      }),
    ];
    const vs = distinctValueSets(states);
    // One entry per distinct slug — NOT per value_set_id.
    expect(vs.map((v) => v.key)).toEqual(["class/lkf1980", "class/lkf1995"]);
    // The collapsed edition's usages are the UNION across its ids' variants.
    const lkf1980 = vs[0];
    expect(lkf1980.classificationSlug).toBe("lkf1980");
    expect(lkf1980.usages.map((u) => u.variant).sort()).toEqual([
      "doda",
      "fodda",
    ]);
  });

  it("buckets a null value_set_id as its own 'no value set' entry", () => {
    const states = [
      state({ value_set_id: null, variant: "a" }),
      state({ value_set_id: 5, variant: "a" }),
      state({ value_set_id: null, variant: "b" }),
    ];
    const vs = distinctValueSets(states);
    expect(vs.map((v) => v.key)).toEqual(["id/none", "id/5"]);
    const nullVs = vs.find((v) => v.key === "id/none");
    expect(nullVs?.variants.sort()).toEqual(["a", "b"]);
  });

  it("exposes each entry's overall span (the view's non-classification disambiguator)", () => {
    // Two plain value sets sharing a version label ("Kommun historisk" ×N): the
    // helper must carry each entry's outer min(valid_from)…max(valid_to) so the
    // view can disambiguate the otherwise-identical rows by span.
    const states = [
      state({
        value_set_id: 1,
        value_set_version_label: "Kommun historisk",
        variant: "a",
        valid_from: "1968-01-01",
        valid_to: "1970-12-31",
      }),
      state({
        value_set_id: 2,
        value_set_version_label: "Kommun historisk",
        variant: "a",
        valid_from: "1971-01-01",
        valid_to: "1973-12-31",
      }),
    ];
    const vs = distinctValueSets(states);
    expect(vs.map((v) => v.overallSpan)).toEqual([
      { from: "1968-01-01", to: "1970-12-31" },
      { from: "1971-01-01", to: "1973-12-31" },
    ]);
  });

  it("carries classification_slug + version label from the representative state", () => {
    const states = [
      state({
        value_set_id: 1,
        classification_slug: "lkf2007",
        value_set_version_label: "LKF",
        variant: "a",
      }),
      state({
        value_set_id: 2,
        classification_slug: null,
        value_set_version_label: "Kommun historisk",
        variant: "a",
      }),
    ];
    const vs = distinctValueSets(states);
    expect(vs[0].classificationSlug).toBe("lkf2007");
    expect(vs[1].classificationSlug).toBeNull();
    expect(vs[1].versionLabel).toBe("Kommun historisk");
  });

  it("lists which variants use a value set (the cross-variant case)", () => {
    const states = [
      state({ value_set_id: 1, variant: "doda", valid_from: "1983-01-01" }),
      state({ value_set_id: 1, variant: "fodda", valid_from: "1983-01-01" }),
    ];
    const vs = distinctValueSets(states);
    expect(vs[0].usages.map((u) => u.variant).sort()).toEqual([
      "doda",
      "fodda",
    ]);
  });

  it("collapses contiguous same-year states into one span (M20)", () => {
    // The AGI annual-state design: 8 annual states under one value set / variant
    // fuse into a single 1983–1990 span.
    const years = [1983, 1984, 1985, 1986, 1987, 1988, 1989, 1990];
    const states = years.map((y) =>
      state({
        value_set_id: 1,
        variant: "doda",
        valid_from: `${y}-01-01`,
        valid_to: `${y}-12-31`,
      }),
    );
    const vs = distinctValueSets(states);
    expect(vs[0].usages[0].spans).toEqual([
      { from: "1983-01-01", to: "1990-12-31" },
    ]);
  });

  it("carries technical changes inside a folded value-set span (#743)", () => {
    const states = [
      state({
        state_id: 1,
        value_set_id: 1,
        variant: "doda",
        valid_from: "2010-01-01",
        valid_to: "2010-12-31",
        data_type: "int",
        delivery_column_name: "KOMMUN",
      }),
      state({
        state_id: 2,
        value_set_id: 1,
        variant: "doda",
        valid_from: "2011-01-01",
        valid_to: "2011-12-31",
        data_type: "bigint",
        delivery_column_name: "KOMMUN_ID",
      }),
    ];
    const vs = distinctValueSets(states);
    expect(vs[0].usages[0].spans).toEqual([
      {
        from: "2010-01-01",
        to: "2011-12-31",
        changes: [
          {
            at: "2011-01-01",
            notes: ["type int -> bigint", "column KOMMUN -> KOMMUN_ID"],
          },
        ],
      },
    ]);
  });

  it("does not report technical changes for same-state monthly windows", () => {
    const states = [
      state({
        state_id: 10,
        value_set_id: 1,
        variant: "individer",
        valid_from: "2020-01-01",
        valid_to: "2020-01-31",
        delivery_column_name: "LonFinkJan",
      }),
      state({
        state_id: 10,
        value_set_id: 1,
        variant: "individer",
        valid_from: "2020-02-01",
        valid_to: "2020-02-29",
        delivery_column_name: "LonFinkFeb",
      }),
    ];
    const vs = distinctValueSets(states);
    expect(vs[0].usages[0].spans).toEqual([
      { from: "2020-01-01", to: "2020-02-29" },
    ]);
  });

  it("does not report technical changes for overlapping alternatives", () => {
    const states = [
      state({
        value_set_id: 1,
        variant: "individer",
        valid_from: "2020-01-01",
        valid_to: "2020-12-31",
        delivery_column_name: "A",
      }),
      state({
        value_set_id: 1,
        variant: "individer",
        valid_from: "2020-06-01",
        valid_to: "2021-12-31",
        delivery_column_name: "B",
      }),
    ];
    const vs = distinctValueSets(states);
    expect(vs[0].usages[0].spans).toEqual([
      { from: "2020-01-01", to: "2021-12-31" },
    ]);
  });

  it("keeps the span-end predecessor after a contained overlap", () => {
    const states = [
      state({
        state_id: 1,
        value_set_id: 1,
        variant: "individer",
        valid_from: "2020-01-01",
        valid_to: "2021-12-31",
        data_type: "int",
        delivery_column_name: "A",
      }),
      state({
        state_id: 2,
        value_set_id: 1,
        variant: "individer",
        valid_from: "2021-01-01",
        valid_to: "2021-06-30",
        data_type: "char",
        delivery_column_name: "B",
      }),
      state({
        state_id: 3,
        value_set_id: 1,
        variant: "individer",
        valid_from: "2022-01-01",
        valid_to: "2022-12-31",
        data_type: "bigint",
        delivery_column_name: "C",
      }),
    ];
    const vs = distinctValueSets(states);
    expect(vs[0].usages[0].spans).toEqual([
      {
        from: "2020-01-01",
        to: "2022-12-31",
        changes: [
          {
            at: "2022-01-01",
            notes: ["type int -> bigint", "column A -> C"],
          },
        ],
      },
    ]);
  });

  it("a gap year splits a span in two", () => {
    const states = [
      state({
        value_set_id: 1,
        variant: "doda",
        valid_from: "2000-01-01",
        valid_to: "2000-12-31",
      }),
      // 2001 missing → gap.
      state({
        value_set_id: 1,
        variant: "doda",
        valid_from: "2002-01-01",
        valid_to: "2002-12-31",
      }),
    ];
    const vs = distinctValueSets(states);
    expect(vs[0].usages[0].spans).toEqual([
      { from: "2000-01-01", to: "2000-12-31" },
      { from: "2002-01-01", to: "2002-12-31" },
    ]);
  });

  it("does not merge across different variants (spans are per-variant)", () => {
    const states = [
      state({
        value_set_id: 1,
        variant: "doda",
        valid_from: "2000-01-01",
        valid_to: "2000-12-31",
      }),
      state({
        value_set_id: 1,
        variant: "fodda",
        valid_from: "2001-01-01",
        valid_to: "2001-12-31",
      }),
    ];
    const vs = distinctValueSets(states);
    const doda = vs[0].usages.find((u) => u.variant === "doda");
    const fodda = vs[0].usages.find((u) => u.variant === "fodda");
    expect(doda?.spans).toEqual([{ from: "2000-01-01", to: "2000-12-31" }]);
    expect(fodda?.spans).toEqual([{ from: "2001-01-01", to: "2001-12-31" }]);
  });

  it("keeps the open-ended ceiling on a still-delivered span", () => {
    const states = [
      state({
        value_set_id: 1,
        variant: "doda",
        valid_from: "2016-01-01",
        valid_to: "9999-12-31",
      }),
    ];
    const vs = distinctValueSets(states);
    expect(vs[0].usages[0].spans).toEqual([
      { from: "2016-01-01", to: "9999-12-31" },
    ]);
  });

  it("collapses contiguous years across the UNION of ids in one classification edition (M20)", () => {
    // Two distinct value_set_ids share `lkf1980` (the M13 collapse) and deliver
    // adjacent years (1980, 1981) under the SAME variant. The per-variant M20
    // collapse runs over the UNION of those ids' states, so they fuse into ONE
    // span — not one per id (which would leave two adjacent rows).
    const states = [
      state({
        value_set_id: 100,
        classification_slug: "lkf1980",
        variant: "doda",
        valid_from: "1980-01-01",
        valid_to: "1980-12-31",
      }),
      state({
        value_set_id: 101, // distinct id, SAME edition + variant + adjacent year
        classification_slug: "lkf1980",
        variant: "doda",
        valid_from: "1981-01-01",
        valid_to: "1981-12-31",
      }),
    ];
    const vs = distinctValueSets(states);
    expect(vs).toHaveLength(1);
    expect(vs[0].usages).toHaveLength(1);
    expect(vs[0].usages[0].spans).toEqual([
      { from: "1980-01-01", to: "1981-12-31" },
    ]);
  });

  it("collapseSpans: overlapping windows extend into one span", () => {
    // Two states whose windows OVERLAP (not merely back-to-back) fuse into a
    // single span spanning the outer bounds.
    const states = [
      state({
        value_set_id: 1,
        variant: "doda",
        valid_from: "2000-01-01",
        valid_to: "2003-12-31",
      }),
      state({
        value_set_id: 1,
        variant: "doda",
        valid_from: "2002-01-01", // starts INSIDE the first window
        valid_to: "2005-12-31",
      }),
    ];
    const vs = distinctValueSets(states);
    expect(vs[0].usages[0].spans).toEqual([
      { from: "2000-01-01", to: "2005-12-31" },
    ]);
  });

  it("collapseSpans: a real >1-day gap splits into two spans", () => {
    // A multi-day gap between windows (not a same-day continuation) starts a new
    // span — the day-after adjacency test must NOT fuse across it.
    const states = [
      state({
        value_set_id: 1,
        variant: "doda",
        valid_from: "2000-01-01",
        valid_to: "2000-06-30",
      }),
      state({
        value_set_id: 1,
        variant: "doda",
        valid_from: "2000-08-01", // a one-month gap after 2000-06-30
        valid_to: "2000-12-31",
      }),
    ];
    const vs = distinctValueSets(states);
    expect(vs[0].usages[0].spans).toEqual([
      { from: "2000-01-01", to: "2000-06-30" },
      { from: "2000-08-01", to: "2000-12-31" },
    ]);
  });

  it("collapseSpans: two open-ended states under one (value set, variant) → ONE span (FIX A)", () => {
    // Regression for the `dayAfter("9999-12-31")` year-10000 overflow: two
    // still-delivered states (both `valid_to: 9999-12-31`) for one (value set,
    // variant) MUST collapse to a single open-ended span. Before the fix the
    // overflowed day-after sorted BELOW any real `valid_from`, so the second
    // open-ended state wrongly opened a spurious "since 2020" span beside the
    // "since 2016" one.
    const states = [
      state({
        value_set_id: 1,
        variant: "doda",
        valid_from: "2016-01-01",
        valid_to: "9999-12-31",
      }),
      state({
        value_set_id: 1,
        variant: "doda",
        valid_from: "2020-01-01",
        valid_to: "9999-12-31",
      }),
    ];
    const vs = distinctValueSets(states);
    expect(vs[0].usages[0].spans).toEqual([
      { from: "2016-01-01", to: "9999-12-31" },
    ]);
  });
});
