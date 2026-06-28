import { describe, expect, it } from "vitest";
import type {
  CatalogNode,
  GraphState,
  StatesResponse,
  VariableStateModel,
} from "./api";
import type {
  PickerBandFacets,
  PickerDimension,
  PickerRepresentation,
} from "./catalog";
import {
  addWindowBounds,
  axisNoun,
  bandLabeling,
  bindingChildren,
  breadcrumbs,
  catalogHref,
  classGroupHref,
  clusterBands,
  coexistingColumns,
  commonLabelStem,
  coverageFromStates,
  DATA_BROWSER_LABEL,
  deriveType,
  distinctValueSets,
  encodeCodesParam,
  facetLabelJoin,
  foldText,
  formatDataType,
  formatStateWindow,
  formatWindow,
  fqidSegments,
  grainsFromStates,
  groupHref,
  groupLinkFromFocus,
  humanizeClassificationSlug,
  labelSuffix,
  leafSlug,
  matchesFilter,
  memberCoverageUnion,
  memberKey,
  narrowCatalogNode,
  narrowStatesByModifier,
  nodeLabel,
  parseCodesParam,
  pickerFilterDimensions,
  pickerLabeling,
  pickerRepresentations,
  pickerRowPasses,
  pickerWindowYears,
  qualifierFromFocus,
  rankFilter,
  registerPrefixOf,
  representationInWindow,
  representationsCollapse,
  representationsFromStates,
  routeBreadcrumbs,
  rowAddPeriod,
  rowFacet,
  valueSetKeyForColumn,
  variantSeg,
  windowTitle,
  YEARLESS_VALID_FROM,
  yearOf,
} from "./catalog";
import { VALUE_SET_VERSION_NONE } from "./period";
import type { Route } from "./router.svelte";

// #819: a group's axis is now `{name, label}`. Tests key on the stable name and
// don't assert the label here, so default the label to the name. Wraps the bare
// axis-name lists the helper tests build.
function ax(...names: string[]): { name: string; label: string }[] {
  return names.map((name) => ({ name, label: name }));
}

// Minimal VariableStateModel — only the fields deriveType/distinctVersions read.
function state(over: Partial<VariableStateModel>): VariableStateModel {
  return {
    state_id: 1,
    variant: "v",
    variant_label: null,
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
  name: "Statistiska Centralbyrån",
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
    expect(nodeLabel(provider)).toBe("Statistiska Centralbyrån");
    expect(nodeLabel(register)).toBe("scb/lisa"); // name is null → fqid
    expect(nodeLabel(classification)).toBe("Education");
  });
});

describe("axisNoun", () => {
  it("pluralizes the group's single facet axis, else falls back to members", () => {
    // Single-axis token groups pluralize their axis (never a re-hardcoded
    // "vintages"). Classification umbrellas are now AXIS-LESS (`axes: []`, #516)
    // → the empty-axis fallback "members" is the umbrella's member noun.
    expect(axisNoun(ax("dimension"))).toBe("dimensions");
    expect(axisNoun(ax("vintage"))).toBe("vintages");
    expect(axisNoun([])).toBe("members"); // no axis (umbrella) → generic fallback
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
    // `/` separators between the route segments survive (per-segment encoding).
    expect(groupHref("scb/lsön", "a/b")).toBe(
      "/catalog/group/scb/ls%C3%B6n/a%2Fb",
    );
  });
});

describe("classGroupHref (#756)", () => {
  it("builds the /catalog/group/class/<key> route from a bare key", () => {
    // The classification sibling of groupHref: the literal `class` route, no
    // provider/register — a classification umbrella is catalog-global.
    expect(classGroupHref("sun")).toBe("/catalog/group/class/sun");
  });

  it("percent-encodes the key", () => {
    expect(classGroupHref("a/b")).toBe("/catalog/group/class/a%2Fb");
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

describe("routeBreadcrumbs", () => {
  // The topbar trail is STRUCTURAL (raw segments + the data-browser root); the
  // routed page owns its rich header. The LAST crumb is always the current page
  // (no href — Breadcrumbs renders it as aria-current="page").
  const browserRoot = { label: DATA_BROWSER_LABEL, href: catalogHref("") };

  it("home → a single un-linked Home crumb", () => {
    expect(routeBreadcrumbs({ name: "home" })).toEqual([{ label: "Home" }]);
  });

  it("root → a single un-linked data-browser crumb (it IS the current page)", () => {
    expect(routeBreadcrumbs({ name: "root" })).toEqual([
      { label: DATA_BROWSER_LABEL },
    ]);
  });

  it("catalog-node → browser root + cumulative ancestor links, leaf un-linked", () => {
    const trail = routeBreadcrumbs({
      name: "catalog-node",
      fqidPath: "scb/lisa/kon",
    });
    expect(trail).toHaveLength(4);
    expect(trail[0]).toEqual(browserRoot);
    expect(trail[1]).toEqual({ label: "scb", href: catalogHref("scb") });
    expect(trail[2]).toEqual({
      label: "lisa",
      href: catalogHref("scb/lisa"),
    });
    expect(trail[3]).toEqual({ label: "kon", href: undefined });
    // Spelled-out hrefs match the documented contract.
    expect(trail[1].href).toBe("/catalog/scb");
    expect(trail[2].href).toBe("/catalog/scb/lisa");
  });

  it("catalog-node single-segment → browser root + the un-linked leaf", () => {
    const trail = routeBreadcrumbs({ name: "catalog-node", fqidPath: "scb" });
    expect(trail).toHaveLength(2);
    expect(trail[0]).toEqual(browserRoot);
    expect(trail[1]).toEqual({ label: "scb", href: undefined });
  });

  it("group → browser root + split provider/register hops + the un-linked key", () => {
    const trail = routeBreadcrumbs({
      name: "group",
      provider: "scb",
      register: "lisa",
      key: "ink",
    });
    expect(trail).toHaveLength(4);
    expect(trail[0]).toEqual(browserRoot);
    expect(trail[1]).toEqual({ label: "scb", href: catalogHref("scb") });
    expect(trail[2]).toEqual({
      label: "lisa",
      href: catalogHref("scb/lisa"),
    });
    expect(trail[3]).toEqual({ label: "ink" });
  });

  it("class-group → browser root + a linked class hop + the un-linked key", () => {
    const trail = routeBreadcrumbs({ name: "class-group", key: "sun" });
    expect(trail).toHaveLength(3);
    expect(trail[0]).toEqual(browserRoot);
    expect(trail[1]).toEqual({ label: "class", href: catalogHref("class") });
    expect(trail[2]).toEqual({ label: "sun" });
  });

  it("search / project → a single un-linked crumb rooted where they sit", () => {
    expect(routeBreadcrumbs({ name: "search" })).toEqual([{ label: "Search" }]);
    expect(routeBreadcrumbs({ name: "project" })).toEqual([
      { label: "Project" },
    ]);
  });

  it("doc → a Docs hop + the un-linked identifier", () => {
    const trail = routeBreadcrumbs({ name: "doc", identifier: "x" });
    expect(trail).toHaveLength(2);
    expect(trail[1]).toEqual({ label: "x" });
  });

  it("not-found → just the browser root", () => {
    expect(routeBreadcrumbs({ name: "not-found", path: "/x" })).toEqual([
      browserRoot,
    ]);
  });

  it("invariant: the LAST crumb is the current page (href === undefined)", () => {
    // Breadcrumbs renders the final item as aria-current="page" with no link, so
    // every PAGE variant's trail ends on an href-less crumb. `not-found` is the
    // documented exception: it returns the bare `browserRoot` (a recovery LINK
    // back to the data browser, not a current-page crumb), so it's excluded here
    // and covered by its own assertion above.
    const variants: Route[] = [
      { name: "home" },
      { name: "root" },
      { name: "catalog-node", fqidPath: "scb/lisa/kon" },
      { name: "catalog-node", fqidPath: "scb" },
      { name: "group", provider: "scb", register: "lisa", key: "ink" },
      { name: "class-group", key: "sun" },
      { name: "search" },
      { name: "project" },
      { name: "doc", identifier: "x" },
    ];
    for (const route of variants) {
      const trail = routeBreadcrumbs(route);
      expect(trail.at(-1)?.href).toBeUndefined();
    }
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

import type { BindingGroupRef, ConceptGroup, VariableGraphNode } from "./api";
import {
  axisValues,
  countFoldedMembers,
  distinctMemberCount,
  foldGroupedRows,
  groupFilterKeys,
  memberAt,
  membersHaveUniqueCoords,
} from "./catalog";

/** A minimal `VariableGraphNode` — only the fields the #670 focus-node header
 * helpers read (`facets` / `group_label` / `fqid`). */
function focusNode(over: Partial<VariableGraphNode> = {}): VariableGraphNode {
  return {
    kind: "variable",
    id: "v1",
    fqid: "scb/lisa/agi1astsni2007g",
    label: "Näringsgren, största förvärvskälla",
    group_key: null,
    group_label: null,
    definition: null,
    description: null,
    facets: [],
    states: [],
    same_as: [],
    ...over,
  };
}

function group(over: Partial<ConceptGroup>): ConceptGroup {
  return {
    key: "ink",
    label: "Inkomst",
    source: "token",
    axes: ax("month"),
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
  it("carries the label/key plus every member's name, FQID, leaf slug, delivery column, and facet label/value (#322, #674, #819)", () => {
    expect(groupFilterKeys(group({}))).toEqual([
      "Inkomst",
      "ink",
      // januari member: name, fqid, leaf slug, (no delivery column), facet label+value
      "Inkomst i januari",
      "scb/lisa/inkjan",
      "inkjan",
      undefined,
      "januari",
      "01",
      // februari member
      "Inkomst i februari",
      "scb/lisa/inkfeb",
      "inkfeb",
      undefined,
      "februari",
      "02",
    ]);
  });

  it("indexes a representation member's delivery column + facet label so a column/label hunt surfaces the folded group (#819 FIX D)", () => {
    // The iot disponibel-inkomst case: two members on ONE variable distinguished
    // by delivery_column, each carrying a curated kapitalvinst facet. A
    // target-hunt for the column `CDISP5` OR the human label "Exkl. kapitalvinst"
    // must match the group (neither is in the shared name/fqid).
    const repGroup = group({
      key: "disp",
      label: "Disponibel inkomst",
      axes: ax("kapitalvinst"),
      members: [
        {
          fqid: "scb/iot/disp",
          name: "Disponibel inkomst",
          delivery_column: "CDISP",
          facets: [
            {
              axis: "kapitalvinst",
              value: "inkl",
              label: "Inkl. kapitalvinst",
            },
          ],
        },
        {
          fqid: "scb/iot/disp",
          name: "Disponibel inkomst",
          delivery_column: "CDISP5",
          facets: [
            {
              axis: "kapitalvinst",
              value: "exkl",
              label: "Exkl. kapitalvinst",
            },
          ],
        },
      ],
    } as unknown as Partial<ConceptGroup>);
    const keys = groupFilterKeys(repGroup);
    expect(keys).toContain("CDISP5");
    expect(keys).toContain("Exkl. kapitalvinst");
    // And rankFilter actually surfaces the group on those needles.
    const rows = [{ id: "other" }, { id: "disp-group", group: repGroup }];
    const keysOf = (r: (typeof rows)[number]): (string | null | undefined)[] =>
      "group" in r && r.group ? groupFilterKeys(r.group) : ["unrelated"];
    expect(rankFilter(rows, "CDISP5", keysOf)[0].id).toBe("disp-group");
    expect(rankFilter(rows, "exkl. kapitalvinst", keysOf)[0].id).toBe(
      "disp-group",
    );
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
    axes: ax("month", "rank"),
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

describe("membersHaveUniqueCoords (#819 FIX C)", () => {
  it("is true when every member occupies a distinct coordinate (the matrix is lossless)", () => {
    const matrix = group({
      axes: ax("month", "rank"),
      members: [
        {
          fqid: "scb/lisa/a",
          name: null,
          facets: [
            { axis: "month", value: "01", label: "jan" },
            { axis: "rank", value: "1", label: "1" },
          ],
        },
        {
          fqid: "scb/lisa/b",
          name: null,
          facets: [
            { axis: "month", value: "01", label: "jan" },
            { axis: "rank", value: "2", label: "2" },
          ],
        },
      ],
    });
    expect(membersHaveUniqueCoords(matrix, ax("month", "rank"))).toBe(true);
  });

  it("is false when two members share a full 2-axis coordinate (the matrix would drop one)", () => {
    // Representation members: same (month, rank) coords, distinguished only by
    // delivery_column — the exact shape FIX C must route through the navigator.
    const collide = group({
      axes: ax("month", "rank"),
      members: [
        {
          fqid: "scb/iot/din8",
          name: null,
          delivery_column: "DIN83",
          facets: [
            { axis: "month", value: "01", label: "jan" },
            { axis: "rank", value: "1", label: "1" },
          ],
        },
        {
          fqid: "scb/iot/din8",
          name: null,
          delivery_column: "DIN84",
          facets: [
            { axis: "month", value: "01", label: "jan" },
            { axis: "rank", value: "1", label: "1" },
          ],
        },
      ],
    } as unknown as Partial<ConceptGroup>);
    expect(membersHaveUniqueCoords(collide, ax("month", "rank"))).toBe(false);
  });

  it("does not alias distinct vectors by concatenation (separator safety)", () => {
    // ["ab",""] vs ["a","b"] both concatenate to "ab" without a separator — must
    // stay distinct so a real collision isn't masked / a non-collision isn't faked.
    const g = group({
      axes: ax("x", "y"),
      members: [
        {
          fqid: "scb/a/1",
          name: null,
          facets: [{ axis: "x", value: "ab", label: "ab" }],
        },
        {
          fqid: "scb/a/2",
          name: null,
          facets: [
            { axis: "x", value: "a", label: "a" },
            { axis: "y", value: "b", label: "b" },
          ],
        },
      ],
    } as unknown as Partial<ConceptGroup>);
    expect(membersHaveUniqueCoords(g, ax("x", "y"))).toBe(true);
  });
});

describe("qualifierFromFocus (#670, graph-sourced #678)", () => {
  it("joins the focus node's facet labels (multiple facets)", () => {
    const focus = focusNode({
      facets: [
        { axis: "source", value: "agi", label: "AGI" },
        { axis: "edition", value: "sni2007", label: "2007 SNI edition" },
      ],
      group_label: "Näringsgren, största förvärvskälla",
    });
    expect(qualifierFromFocus(focus, "scb/lisa/agi1astsni2007g")).toEqual({
      text: "AGI · 2007 SNI edition",
      kind: "facets",
    });
  });

  it("handles a single facet", () => {
    const focus = focusNode({
      facets: [{ axis: "source", value: "ku", label: "KU" }],
      group_label: "Näringsgren",
    });
    expect(qualifierFromFocus(focus, "scb/lisa/ku1astsni2002g")).toEqual({
      text: "KU",
      kind: "facets",
    });
  });

  it("returns null for an UNGROUPED focus (no facets, no group_label)", () => {
    expect(qualifierFromFocus(focusNode(), "scb/lisa/notamember")).toBeNull();
  });

  it("falls back to the CANONICAL focus slug for a GROUPED facet-less focus (edge group split sibling)", () => {
    // M10's exact case: an edge group whose focus carries a `group_label` but no
    // facets, so the slug is the only differentiator between the siblings. The
    // focus node's own fqid IS the canonical identity, so the slug reads it; the
    // leaf arg is only the fallback when the focus carries no fqid.
    const focus = focusNode({
      fqid: "scb/lisa/agi1astsni2007g",
      facets: [],
      group_label: "Näringsgren",
    });
    expect(qualifierFromFocus(focus, "scb/lisa/agi1astsni2007g")).toEqual({
      text: "agi1astsni2007g",
      kind: "slug",
    });
    const focusB = focusNode({
      fqid: "scb/lisa/ku1astsni",
      facets: [],
      group_label: "Näringsgren",
    });
    expect(qualifierFromFocus(focusB, "scb/lisa/ku1astsni")).toEqual({
      text: "ku1astsni",
      kind: "slug",
    });
  });

  it("uses the CANONICAL focus slug for the fallback, not the leaf alias (same_as)", () => {
    // The focus node is keyed on the RESOLVED (canonical) target; opened via a
    // same_as alias, the slug fallback must show the CANONICAL sibling slug
    // (focus.fqid), so the alias page and the canonical page read the SAME
    // technical identifier (#670 Codex-P2 parity) — NOT the alias leaf slug.
    const focus = focusNode({
      fqid: "scb/rams/inkjan",
      facets: [],
      group_label: "Näringsgren",
    });
    expect(qualifierFromFocus(focus, "scb/lisa/agi1astsni2007g")).toEqual({
      text: "inkjan",
      kind: "slug",
    });
  });

  it("falls back to the LEAF fqid for the slug when the focus carries no fqid", () => {
    // A focus node's fqid can be null (it isn't the navigation key); the leaf arg
    // then supplies the slug.
    const focus = focusNode({
      fqid: null,
      facets: [],
      group_label: "Näringsgren",
    });
    expect(qualifierFromFocus(focus, "scb/lisa/agi1astsni2007g")).toEqual({
      text: "agi1astsni2007g",
      kind: "slug",
    });
  });

  it("returns null for a null/absent focus (graph unresolved)", () => {
    expect(qualifierFromFocus(null, "scb/lisa/kon")).toBeNull();
    expect(qualifierFromFocus(undefined, "scb/lisa/kon")).toBeNull();
  });
});

describe("memberKey (#819 composite member key)", () => {
  it("composes (fqid, delivery_column) so two reps of one variable differ", () => {
    // Two members on one variable (two delivery columns) must yield DISTINCT keys
    // — a fqid-only key would collide and Svelte would drop the second.
    const a = { fqid: "scb/iot/dispink", delivery_column: "dispink_inkl" };
    const b = { fqid: "scb/iot/dispink", delivery_column: "dispink_exkl" };
    expect(memberKey(a)).not.toBe(memberKey(b));
    expect(memberKey(a)).toBe("scb/iot/dispink::dispink_inkl");
  });

  it("treats a null/absent delivery_column as the empty discriminator", () => {
    // A whole-variable member (delivery_column null) keys stably on its fqid;
    // null and undefined map to the same empty discriminator.
    expect(memberKey({ fqid: "scb/lisa/kon", delivery_column: null })).toBe(
      "scb/lisa/kon::",
    );
    expect(memberKey({ fqid: "scb/lisa/kon" })).toBe("scb/lisa/kon::");
  });
});

describe("facetLabelJoin", () => {
  it("joins facet labels with ' · '", () => {
    expect(
      facetLabelJoin([{ label: "AGI" }, { label: "2007 SNI edition" }]),
    ).toBe("AGI · 2007 SNI edition");
  });

  it("returns a single facet's label unchanged and '' for none", () => {
    expect(facetLabelJoin([{ label: "KU" }])).toBe("KU");
    expect(facetLabelJoin([])).toBe("");
  });
});

describe("yearOf", () => {
  it("extracts the leading 4-digit year of an ISO bound", () => {
    expect(yearOf("2010-01-01")).toBe(2010);
    expect(yearOf("9999-12-31")).toBe(9999);
  });

  it("returns null for a blank/non-leading-4-digit bound", () => {
    expect(yearOf("")).toBeNull();
    expect(yearOf("not-a-date")).toBeNull();
  });
});

describe("groupLinkFromFocus (#670, graph-sourced #678)", () => {
  const ref: BindingGroupRef = {
    provider: "scb",
    register: "lisa",
    key: "naringsgren",
  };
  const grouped = focusNode({
    group_label: "Näringsgren, största förvärvskälla",
    group_key: "naringsgren",
  });

  it("returns the focus group_label + the group-subject href from the leaf ref", () => {
    expect(groupLinkFromFocus(grouped, ref)).toEqual({
      label: "Näringsgren, största förvärvskälla",
      href: "/catalog/group/scb/lisa/naringsgren",
    });
  });

  it("returns null when the focus is ungrouped (no group_label)", () => {
    expect(groupLinkFromFocus(focusNode(), ref)).toBeNull();
  });

  it("returns null when the leaf carries no group ref", () => {
    expect(groupLinkFromFocus(grouped, null)).toBeNull();
  });

  it("returns null for a null focus (graph unresolved)", () => {
    expect(groupLinkFromFocus(null, ref)).toBeNull();
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
      axes: ax("vintage"),
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

  it("counts a representation group's members in DISTINCT-variable units (#819)", () => {
    // Two representation members on ONE variable (one fqid, distinct delivery
    // columns) — the group must count as 1 variable, not 2 rows.
    const rep = group({
      key: "disprep",
      members: [
        {
          fqid: "scb/iot/disponibel-inkomst",
          name: "Disp. inkomst (CDISP)",
          facets: [{ axis: "rep", value: "CDISP", label: "CDISP" }],
        },
        {
          fqid: "scb/iot/disponibel-inkomst",
          name: "Disp. inkomst (CDISP5)",
          facets: [{ axis: "rep", value: "CDISP5", label: "CDISP5" }],
        },
      ],
    });
    expect(distinctMemberCount(rep.members)).toBe(1);
    const rows = foldGroupedRows(
      [{ fqid: "scb/iot/disponibel-inkomst" }],
      [rep],
    );
    expect(countFoldedMembers(rows)).toBe(1); // 2 representation rows = 1 variable
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

describe("narrowStatesByModifier (#678: picker honors the active narrowing)", () => {
  // A variable with two variants and two value-set versions — the picker should
  // offer only the rows consistent with whichever modifier is active.
  const states = [
    state({
      variant: "lastbilar",
      delivery_column_name: "SNI2002",
      value_set_version_label: "SNI 2002",
    }),
    state({
      variant: "bussar",
      delivery_column_name: "SNI2002",
      value_set_version_label: "SNI 2002",
    }),
    state({
      variant: "lastbilar",
      delivery_column_name: "SNI2007",
      value_set_version_label: "SNI 2007",
    }),
    state({
      variant: "personbilar",
      delivery_column_name: "SNI2002",
      value_set_version_label: "",
    }),
  ];

  it("no modifier active → states pass through unchanged (full history)", () => {
    expect(narrowStatesByModifier(states, null, null)).toBe(states);
  });

  it("an active ?variant keeps only that variant's states", () => {
    const narrowed = narrowStatesByModifier(states, "lastbilar", null);
    expect(narrowed.map((s) => s.variant)).toEqual(["lastbilar", "lastbilar"]);
    // …so "select all" over the picker rows only adds rows for the narrowed
    // variant (lastbilar carries two columns → two rows, both lastbilar; the
    // bussar / personbilar rows are gone).
    const rows = pickerRepresentations(narrowed);
    expect(rows.every((r) => r.variant === "lastbilar")).toBe(true);
    expect(rows.map((r) => r.key)).toEqual([
      "lastbilar::SNI2002",
      "lastbilar::SNI2007",
    ]);
  });

  it("an active ?value_set_version keeps only that version's states", () => {
    const narrowed = narrowStatesByModifier(states, null, "SNI 2007");
    expect(narrowed.map((s) => s.value_set_version_label)).toEqual([
      "SNI 2007",
    ]);
    expect(pickerRepresentations(narrowed).map((r) => r.key)).toEqual([
      "lastbilar::SNI2007",
    ]);
  });

  it("variant AND version both narrow (intersection)", () => {
    const narrowed = narrowStatesByModifier(states, "lastbilar", "SNI 2002");
    expect(narrowed).toHaveLength(1);
    expect(narrowed[0].variant).toBe("lastbilar");
    expect(narrowed[0].value_set_version_label).toBe("SNI 2002");
  });

  it("the _none version sentinel matches the empty/default label", () => {
    const narrowed = narrowStatesByModifier(
      states,
      null,
      VALUE_SET_VERSION_NONE,
    );
    expect(narrowed.map((s) => s.variant)).toEqual(["personbilar"]);
  });
});

describe("pickerRepresentations (#678 direct picker)", () => {
  it("one row per distinct (variant, delivery column), period spanning its states", () => {
    const states = [
      state({
        state_id: 1,
        variant: "individer",
        delivery_column_name: "Kon",
        valid_from: "2010-01-01",
        valid_to: "2015-12-31",
        value_set_version_label: "1-siffrig",
      }),
      state({
        state_id: 2,
        variant: "individer",
        delivery_column_name: "Kon",
        valid_from: "2016-01-01",
        valid_to: "2020-12-31",
        value_set_version_label: "1-siffrig",
      }),
      state({
        state_id: 3,
        variant: "individer",
        delivery_column_name: "KonDetalj",
        valid_from: "2018-01-01",
        valid_to: "2020-12-31",
        value_set_version_label: "2-siffrig",
      }),
    ];
    const rows = pickerRepresentations(states);
    expect(rows.map((r) => r.key)).toEqual([
      "individer::Kon",
      "individer::KonDetalj",
    ]);
    // The Kon row's span fuses its two states: 2010 – 2020.
    expect(rows[0].column).toBe("Kon");
    expect(rows[0].from).toBe("2010-01-01");
    expect(rows[0].to).toBe("2020-12-31");
    expect(rows[0].period).toBe("2010 – 2020");
    expect(rows[0].wirePeriod).toBe("2010..2020");
  });

  it("the value-set label comes from the latest-era state of the column", () => {
    const states = [
      state({
        state_id: 1,
        variant: "v1",
        delivery_column_name: "Sni",
        valid_from: "2002-01-01",
        valid_to: "2006-12-31",
        value_set_version_label: "SNI 2002",
      }),
      state({
        state_id: 2,
        variant: "v1",
        delivery_column_name: "Sni",
        valid_from: "2007-01-01",
        valid_to: "2020-12-31",
        value_set_version_label: "SNI 2007",
      }),
    ];
    const [row] = pickerRepresentations(states);
    // Latest era (valid_to 2020) → SNI 2007.
    expect(row.valueSetLabel).toBe("SNI 2007");
  });

  it("skips states with a null delivery column (nothing to select)", () => {
    const states = [
      state({ variant: "v1", delivery_column_name: null }),
      state({
        variant: "v1",
        delivery_column_name: "Real",
        valid_from: "2010-01-01",
        valid_to: "2010-12-31",
      }),
    ];
    const rows = pickerRepresentations(states);
    expect(rows).toHaveLength(1);
    expect(rows[0].column).toBe("Real");
    // An ordinary single-column row commits its own column (no rename fold) (#902).
    expect(rows[0].representation).toBe("Real");
    expect(rows[0].renamedColumns).toEqual([]);
  });

  it("an open-ended span renders 'since' and leaves the wire period unset", () => {
    const [row] = pickerRepresentations([
      state({
        variant: "v1",
        delivery_column_name: "Col",
        valid_from: "2010-01-01",
        valid_to: "9999-12-31",
      }),
    ]);
    expect(row.period).toBe("since 2010");
    // No in-grammar token for an unbounded end → source period left unset.
    expect(row.wirePeriod).toBeNull();
  });

  it("a same-year span yields a bare-year wire period", () => {
    const [row] = pickerRepresentations([
      state({
        variant: "v1",
        delivery_column_name: "Col",
        valid_from: "2018-01-01",
        valid_to: "2018-12-31",
      }),
    ]);
    expect(row.wirePeriod).toBe("2018");
  });

  // #678 finding 4: a SUB-ANNUAL span keeps its exact grain on the wire — it must
  // NOT widen to the whole year (which would pull in sibling columns for the rest
  // of the year on resolve).
  it("a single-month span emits the EXACT month token, not the year", () => {
    const [row] = pickerRepresentations([
      state({
        variant: "v1",
        delivery_column_name: "Col",
        valid_from: "2020-01-01",
        valid_to: "2020-01-31",
      }),
    ]);
    expect(row.wirePeriod).toBe("2020-01");
  });

  it("a quarter span emits the EXACT quarter token", () => {
    const [row] = pickerRepresentations([
      state({
        variant: "v1",
        delivery_column_name: "Col",
        valid_from: "2020-07-01",
        valid_to: "2020-09-30",
      }),
    ]);
    expect(row.wirePeriod).toBe("2020-Q3");
  });

  it("a single-day span emits the exact day token", () => {
    const [row] = pickerRepresentations([
      state({
        variant: "v1",
        delivery_column_name: "Col",
        valid_from: "2020-03-15",
        valid_to: "2020-03-15",
      }),
    ]);
    expect(row.wirePeriod).toBe("2020-03-15");
  });

  it("a whole-year multi-year span stays a bare-year range (not ISO dates)", () => {
    const [row] = pickerRepresentations([
      state({
        variant: "v1",
        delivery_column_name: "Col",
        valid_from: "2010-01-01",
        valid_to: "2015-12-31",
      }),
    ]);
    expect(row.wirePeriod).toBe("2010..2015");
  });

  it("a sub-annual multi-month span emits an exact ISO range (not year-rounded)", () => {
    const [row] = pickerRepresentations([
      state({
        variant: "v1",
        delivery_column_name: "Col",
        valid_from: "2020-02-01",
        valid_to: "2020-06-30",
      }),
    ]);
    // No single token covers Feb–Jun; the explicit range preserves the exact span,
    // and the year-aligned collapse does NOT apply (sub-annual endpoints).
    expect(row.wirePeriod).toBe("2020-02-01..2020-06-30");
  });

  // #678 finding 3: a column delivered in DISJOINT windows commits the comma-union
  // (the interrupted-series wire), never one continuous range over the gap years.
  it("a DISJOINT-delivery column emits a comma-list wire (gap years excluded)", () => {
    const [row] = pickerRepresentations([
      state({
        variant: "v1",
        delivery_column_name: "Col",
        valid_from: "2005-01-01",
        valid_to: "2010-12-31",
      }),
      // A real 2011–2014 gap, then a second era.
      state({
        variant: "v1",
        delivery_column_name: "Col",
        valid_from: "2015-01-01",
        valid_to: "2020-12-31",
      }),
    ]);
    expect(row.windows).toEqual([
      { from: "2005-01-01", to: "2010-12-31" },
      { from: "2015-01-01", to: "2020-12-31" },
    ]);
    expect(row.wirePeriod).toBe("2005..2010,2015..2020");
    // The outer span still spans both eras (the display "from..to").
    expect(row.from).toBe("2005-01-01");
    expect(row.to).toBe("2020-12-31");
  });

  it("fuses ADJACENT annual states into ONE window (no spurious comma split)", () => {
    const [row] = pickerRepresentations([
      state({
        variant: "v1",
        delivery_column_name: "Col",
        valid_from: "2018-01-01",
        valid_to: "2018-12-31",
      }),
      // Back-to-back: 2019-01-01 is the day after 2018-12-31 → one continuous window.
      state({
        variant: "v1",
        delivery_column_name: "Col",
        valid_from: "2019-01-01",
        valid_to: "2019-12-31",
      }),
    ]);
    expect(row.windows).toEqual([{ from: "2018-01-01", to: "2019-12-31" }]);
    expect(row.wirePeriod).toBe("2018..2019");
  });

  // #678 inc 2: the widened param accepts the group graph's `GraphState[]` too —
  // same `(variant, delivery_column)` enumeration, but its bounds are nullable.
  it("accepts graph states and normalizes a null end to an open-ended span", () => {
    // A minimal GraphState — null `valid_to` = still delivered. The function must
    // map it to the open-ended `9999-12-31` sentinel so the span renders "since
    // 2010" and the wire period stays unset (no in-grammar token for the end).
    const gstate = (over: Partial<GraphState>): GraphState =>
      ({
        state_id: 1,
        representation_run_id: 1,
        variant: "individer",
        variant_label: null,
        delivery_column_name: null,
        value_set_version_label: "1-siffrig",
        value_set_id: null,
        valid_from: null,
        valid_to: null,
        classification_slug: null,
        ...over,
      }) as GraphState;

    const [row] = pickerRepresentations([
      gstate({
        delivery_column_name: "Kon",
        valid_from: "2010-01-01",
        valid_to: null, // unbounded end → open-ended
      }),
    ]);
    expect(row.key).toBe("individer::Kon");
    expect(row.column).toBe("Kon");
    expect(row.from).toBe("2010-01-01");
    expect(row.to).toBe("9999-12-31");
    expect(row.period).toBe("since 2010");
    expect(row.wirePeriod).toBeNull();
    expect(row.valueSetLabel).toBe("1-siffrig");
  });

  it("normalizes a null graph-state start to the yearless floor (until <year>)", () => {
    const [row] = pickerRepresentations([
      {
        state_id: 2,
        representation_run_id: 1,
        variant: "v1",
        variant_label: null,
        delivery_column_name: "Col",
        value_set_version_label: "",
        value_set_id: null,
        valid_from: null, // unknown start
        valid_to: "2008-12-31",
        classification_slug: null,
      } as GraphState,
    ]);
    expect(row.from).toBe("0001-01-01");
    expect(row.to).toBe("2008-12-31");
    // The one-sided "until <year>" form, never the leaked sentinel year.
    expect(row.period).toBe("until 2008");
  });

  // ── codingsVary: a coding change over time on ONE column (#678) ─────────────
  it("flags codingsVary when a column carried two distinct value_set_ids over time (303→249)", () => {
    // yrkesreg's SUN2020Niva_Old: value-set 303 in 2019, then 249 from 2020 — one
    // column, two distinct codings → the nudge fires.
    const [row] = pickerRepresentations([
      state({
        variant: "forvärvsarbetande",
        delivery_column_name: "SUN2020Niva_Old",
        value_set_id: 303,
        value_set_version_label: "MiS 1996:1",
        valid_from: "2019-01-01",
        valid_to: "2019-12-31",
      }),
      state({
        variant: "forvärvsarbetande",
        delivery_column_name: "SUN2020Niva_Old",
        value_set_id: 249,
        value_set_version_label: "SUN",
        valid_from: "2020-01-01",
        valid_to: "2023-12-31",
      }),
    ]);
    expect(row.codingsVary).toBe(true);
  });

  it("does NOT flag codingsVary when one value_set_id has inconsistent LABELS (the SUN case)", () => {
    // The same id 249 is labelled inconsistently across years/populations ('old' /
    // 'SUN 2020 NivaOld'). Keyed on the reliable id, this is ONE coding → no nudge.
    const [row] = pickerRepresentations([
      state({
        variant: "v1",
        delivery_column_name: "Sun",
        value_set_id: 249,
        value_set_version_label: "SUN 2020 NivaOld",
        valid_from: "2020-01-01",
        valid_to: "2020-12-31",
      }),
      state({
        variant: "v1",
        delivery_column_name: "Sun",
        value_set_id: 249,
        value_set_version_label: "SUN 2000 NivaOld",
        valid_from: "2021-01-01",
        valid_to: "2021-12-31",
      }),
    ]);
    expect(row.codingsVary).toBe(false);
  });

  it("flags codingsVary on a null↔id transition (code-less → coded)", () => {
    // A null value_set_id is its own distinct value, so gaining (or losing) a coding
    // counts as a change.
    const [row] = pickerRepresentations([
      state({
        variant: "v1",
        delivery_column_name: "Col",
        value_set_id: null,
        valid_from: "2018-01-01",
        valid_to: "2018-12-31",
      }),
      state({
        variant: "v1",
        delivery_column_name: "Col",
        value_set_id: 42,
        valid_from: "2019-01-01",
        valid_to: "2019-12-31",
      }),
    ]);
    expect(row.codingsVary).toBe(true);
  });

  it("does NOT flag codingsVary for a single-coding column (one id across years)", () => {
    const [row] = pickerRepresentations([
      state({
        variant: "v1",
        delivery_column_name: "Col",
        value_set_id: 100,
        valid_from: "2018-01-01",
        valid_to: "2018-12-31",
      }),
      state({
        variant: "v1",
        delivery_column_name: "Col",
        value_set_id: 100,
        valid_from: "2019-01-01",
        valid_to: "2019-12-31",
      }),
    ]);
    expect(row.codingsVary).toBe(false);
  });

  // ── variantLabel: the variant display name, slug fallback (#793 contract) ────
  it("carries variantLabel = the variant's name (display), keeping variant = the slug", () => {
    const [row] = pickerRepresentations([
      state({
        variant: "snoskotrar",
        variant_label: "Snöskotrar",
        delivery_column_name: "Sni",
        valid_from: "2010-01-01",
        valid_to: "2015-12-31",
      }),
    ]);
    // The slug stays the identity / add coordinate; the label is the display name.
    expect(row.variant).toBe("snoskotrar");
    expect(row.variantLabel).toBe("Snöskotrar");
  });

  it("falls back variantLabel to the slug when variant_label is null", () => {
    const [row] = pickerRepresentations([
      state({
        variant: "ovriga-fordonsslag",
        variant_label: null,
        delivery_column_name: "Sni",
        valid_from: "2010-01-01",
        valid_to: "2015-12-31",
      }),
    ]);
    expect(row.variantLabel).toBe("ovriga-fordonsslag");
  });

  // ── #902 part 2: collapse an intra-variable SEQUENTIAL RENAME ────────────────
  it("collapses a non-overlapping rename chain into ONE row led by the LATEST column", () => {
    // disponibel-inkomst-familj-2: DINF (1981) → DINF83 (1984–85) → DINF84 (1986–87)
    // → DINF86 (1990–). Distinct columns over NON-overlapping eras = one evolving
    // representation by rename, NOT four co-equal parallel columns.
    const rows = pickerRepresentations([
      state({
        variant: "familj",
        delivery_column_name: "DINF",
        valid_from: "1981-01-01",
        valid_to: "1983-12-31",
      }),
      state({
        variant: "familj",
        delivery_column_name: "DINF83",
        valid_from: "1984-01-01",
        valid_to: "1985-12-31",
      }),
      state({
        variant: "familj",
        delivery_column_name: "DINF84",
        valid_from: "1986-01-01",
        valid_to: "1987-12-31",
      }),
      state({
        variant: "familj",
        delivery_column_name: "DINF86",
        valid_from: "1990-01-01",
        valid_to: "1995-12-31",
      }),
    ]);
    // ONE row, not four — led by the latest-era column DINF86.
    expect(rows).toHaveLength(1);
    expect(rows[0].column).toBe("DINF86");
    expect(rows[0].key).toBe("familj::DINF86");
    // The earlier (superseded) columns ride as the chronological progression hint.
    expect(rows[0].renamedColumns).toEqual(["DINF", "DINF83", "DINF84"]);
    // The collapsed row spans the union of every era (1981 → 1995).
    expect(rows[0].from).toBe("1981-01-01");
    expect(rows[0].to).toBe("1995-12-31");
    // CRITICAL (#902): a folded rename commits `representation: null` — NOT the latest
    // column. Pinning DINF86 over 1981–1995 would be wrong (DINF86 wasn't delivered
    // before 1990); null lets per-period resolution pick the right column per year. The
    // display identity (`column`, the chip) stays the latest column.
    expect(rows[0].representation).toBeNull();
    expect(rows[0].column).toBe("DINF86");
  });

  it("keeps genuinely PARALLEL (overlapping) columns as separate rows", () => {
    // CDISP and CDISP5 both delivered 2010–2020 (inkl. / exkl. kapitalvinst) — a real
    // co-existing choice, NOT a rename: each stays its own selectable row.
    const rows = pickerRepresentations([
      state({
        variant: "individer",
        delivery_column_name: "CDISP",
        valid_from: "2010-01-01",
        valid_to: "2020-12-31",
      }),
      state({
        variant: "individer",
        delivery_column_name: "CDISP5",
        valid_from: "2010-01-01",
        valid_to: "2020-12-31",
      }),
    ]);
    expect(rows.map((r) => r.column).sort()).toEqual(["CDISP", "CDISP5"]);
    // Neither is a rename fold → no superseded-column hint.
    expect(rows.every((r) => r.renamedColumns.length === 0)).toBe(true);
    // A parallel co-existing column commits its OWN column (#902): the author pins which
    // of the co-existing columns they mean.
    expect(rows.every((r) => r.representation === r.column)).toBe(true);
  });

  it("folds the renames but keeps a co-existing parallel PAIR separate", () => {
    // A MIX: A (2008–2010) → B (2011–2014) are a sequential rename (non-overlapping,
    // overlapping nothing else); X and Y both deliver 2015–2020 → a parallel pair. The
    // renames collapse to ONE row (led by B); X and Y each stay their own row (only
    // columns that overlap NOTHING fold — a column overlapping a sibling is parallel).
    const rows = pickerRepresentations([
      state({
        variant: "v",
        delivery_column_name: "A",
        valid_from: "2008-01-01",
        valid_to: "2010-12-31",
      }),
      state({
        variant: "v",
        delivery_column_name: "B",
        valid_from: "2011-01-01",
        valid_to: "2014-12-31",
      }),
      state({
        variant: "v",
        delivery_column_name: "X",
        valid_from: "2015-01-01",
        valid_to: "2020-12-31",
      }),
      state({
        variant: "v",
        delivery_column_name: "Y",
        valid_from: "2015-01-01",
        valid_to: "2020-12-31",
      }),
    ]);
    const byCol = new Map(rows.map((r) => [r.column, r]));
    expect([...byCol.keys()].sort()).toEqual(["B", "X", "Y"]);
    expect(byCol.get("B")?.renamedColumns).toEqual(["A"]);
    expect(byCol.get("X")?.renamedColumns).toEqual([]);
    expect(byCol.get("Y")?.renamedColumns).toEqual([]);
    // The folded rename (B) commits null; the parallel pair (X, Y) commit their own
    // columns (#902).
    expect(byCol.get("B")?.representation).toBeNull();
    expect(byCol.get("X")?.representation).toBe("X");
    expect(byCol.get("Y")?.representation).toBe("Y");
  });

  it("does NOT fold a rename ACROSS variants (rename is one variable+population)", () => {
    // Same column-name lineage but different populations → each variant keeps its own
    // single-column row (per-variant scope), never folded together.
    const rows = pickerRepresentations([
      state({
        variant: "individer",
        delivery_column_name: "Old",
        valid_from: "2010-01-01",
        valid_to: "2014-12-31",
      }),
      state({
        variant: "familj",
        delivery_column_name: "New",
        valid_from: "2015-01-01",
        valid_to: "2020-12-31",
      }),
    ]);
    expect(rows.map((r) => r.key).sort()).toEqual([
      "familj::New",
      "individer::Old",
    ]);
    expect(rows.every((r) => r.renamedColumns.length === 0)).toBe(true);
  });
});

describe("coexistingColumns (#902 shared overlap leaf)", () => {
  it("returns columns whose windows overlap; excludes a sequential rename", () => {
    const set = coexistingColumns([
      {
        delivery_column_name: "A",
        valid_from: "2010-01-01",
        valid_to: "2020-12-31",
      },
      {
        delivery_column_name: "B",
        valid_from: "2012-01-01",
        valid_to: "2018-12-31",
      },
      {
        delivery_column_name: "C",
        valid_from: "2021-01-01",
        valid_to: "2025-12-31",
      },
    ]);
    // A and B overlap; C is wholly after both → a rename, not coexisting.
    expect([...set].sort()).toEqual(["A", "B"]);
  });

  it("treats a null (unbounded) end as overlapping everything after it", () => {
    const set = coexistingColumns([
      { delivery_column_name: "A", valid_from: "2010-01-01", valid_to: null },
      {
        delivery_column_name: "B",
        valid_from: "2030-01-01",
        valid_to: "2031-12-31",
      },
    ]);
    expect([...set].sort()).toEqual(["A", "B"]);
  });

  it("treats a null (unbounded) start as overlapping everything before it", () => {
    // A's null valid_from normalizes to YEARLESS_VALID_FROM (0001), so its window
    // reaches back before B and the two overlap.
    const set = coexistingColumns([
      {
        delivery_column_name: "A",
        valid_from: null,
        valid_to: "2005-12-31",
      },
      {
        delivery_column_name: "B",
        valid_from: "1990-01-01",
        valid_to: "1995-12-31",
      },
    ]);
    expect([...set].sort()).toEqual(["A", "B"]);
  });

  it("treats a fully-null-bounds column as overlapping everything", () => {
    // Both bounds null → window is the full 0001..9999 sentinel span, so it
    // overlaps any other column regardless of era.
    const set = coexistingColumns([
      { delivery_column_name: "A", valid_from: null, valid_to: null },
      {
        delivery_column_name: "B",
        valid_from: "2050-01-01",
        valid_to: "2055-12-31",
      },
    ]);
    expect([...set].sort()).toEqual(["A", "B"]);
  });

  it("treats columns touching at a single boundary instant as co-existing", () => {
    // A ends and B starts on the same day. The inclusive `<=` overlap counts this
    // as co-existing. This documents the explicit design choice so a future `<`
    // "cleanup" can't silently flip it.
    const set = coexistingColumns([
      {
        delivery_column_name: "A",
        valid_from: "2010-01-01",
        valid_to: "2015-12-31",
      },
      {
        delivery_column_name: "B",
        valid_from: "2015-12-31",
        valid_to: "2020-12-31",
      },
    ]);
    expect([...set].sort()).toEqual(["A", "B"]);
  });
});

describe("rowAddPeriod (#678 finding 3: honor the active period on add)", () => {
  // A picker row with explicit ISO bounds + its own full-span wire period. `windows`
  // defaults to ONE continuous window spanning from..to (the common case); disjoint
  // tests override it.
  const row = (
    over: Partial<PickerRepresentation> = {},
  ): PickerRepresentation => {
    const from = over.from ?? "2010-01-01";
    const to = over.to ?? "2020-12-31";
    return {
      key: "v1::Col",
      variant: "v1",
      variantLabel: "v1",
      column: "Col",
      representation: "Col",
      from,
      to,
      windows: [{ from, to }],
      period: "2010 – 2020",
      wirePeriod: "2010..2020",
      valueSetLabel: "",
      codingsVary: false,
      renamedColumns: [],
      ...over,
    };
  };
  // A year window expressed as inclusive ISO bounds (what `addWindowBounds` produces
  // from a year-grain window).
  const yr = (lo: number, hi: number) => ({
    from: `${lo}-01-01`,
    to: `${hi}-12-31`,
  });

  it("no active window → commits the row's own full-span wire period unchanged", () => {
    expect(rowAddPeriod(row(), null)).toBe("2010..2020");
  });

  it("intersects a finite multi-year row with the active window (the user's narrowing wins)", () => {
    // The row spans 2010–2020; the user narrowed to 2015 → add ONLY 2015, not the
    // full history.
    expect(rowAddPeriod(row(), yr(2015, 2015))).toBe("2015");
  });

  it("clamps both sides to a window narrower than the row span", () => {
    expect(rowAddPeriod(row(), yr(2012, 2018))).toBe("2012..2018");
  });

  it("an OPEN-ENDED row (null wirePeriod) clamps to the window → a finite, resolvable period", () => {
    // wirePeriod null would otherwise add a period-unset, unresolved source. With an
    // active window the add lands the window instead.
    const open = row({ to: "9999-12-31", wirePeriod: null });
    expect(rowAddPeriod(open, yr(2020, 2020))).toBe("2020");
  });

  it("an unknown-START row clamps its start to the window", () => {
    const unknownStart = row({ from: "0001-01-01", wirePeriod: null });
    expect(rowAddPeriod(unknownStart, yr(2018, 2019))).toBe("2018..2019");
  });

  it("a window wider than the row span yields the row's own (finite) span", () => {
    expect(rowAddPeriod(row(), yr(2000, 2030))).toBe("2010..2020");
  });

  it("a window wholly OUTSIDE the row span falls back to the row's own wire period", () => {
    // An explicitly-selected dimmed row (out of window) still adds something sensible
    // rather than an empty/inverted intersection.
    expect(rowAddPeriod(row(), yr(2030, 2031))).toBe("2010..2020");
  });

  // #678 finding 1: a SUB-ANNUAL `?period` must commit at its real grain, NOT the
  // collapsed outer year. `addWindowBounds` produces the exact ISO bounds of the
  // selected quarter/term/month, so `rowAddPeriod` honors it.
  it("honors a sub-annual window at its true grain (a quarter stays a quarter, not its year)", () => {
    // The user picked 2020-Q1; the open-ended row clamps to exactly that quarter.
    const open = row({
      from: "2010-01-01",
      to: "9999-12-31",
      wirePeriod: null,
    });
    expect(rowAddPeriod(open, { from: "2020-01-01", to: "2020-03-31" })).toBe(
      "2020-Q1",
    );
  });

  it("honors a sub-annual month window (a long row clamped to a single month)", () => {
    expect(rowAddPeriod(row(), { from: "2015-03-01", to: "2015-03-31" })).toBe(
      "2015-03",
    );
  });

  // #678 finding 3: a row with DISJOINT delivery windows clamped against the active
  // window emits the comma-union (the interrupted-series wire), never a continuous
  // range that would cover the gap years.
  it("emits the comma-list for a DISJOINT-window row (the gap years are not covered)", () => {
    // Delivered 2005–2010 then 2015–2020 (a real 2011–2014 gap). With no narrowing the
    // commit is the comma-union over the two eras, NOT a continuous 2005..2020.
    const disjoint = row({
      from: "2005-01-01",
      to: "2020-12-31",
      windows: [
        { from: "2005-01-01", to: "2010-12-31" },
        { from: "2015-01-01", to: "2020-12-31" },
      ],
      wirePeriod: "2005..2010,2015..2020",
    });
    expect(rowAddPeriod(disjoint, null)).toBe("2005..2010,2015..2020");
  });

  it("clamps each disjoint window into the active window, dropping a window that falls outside", () => {
    const disjoint = row({
      from: "2005-01-01",
      to: "2020-12-31",
      windows: [
        { from: "2005-01-01", to: "2010-12-31" },
        { from: "2015-01-01", to: "2020-12-31" },
      ],
      wirePeriod: "2005..2010,2015..2020",
    });
    // A window over 2008–2017 keeps both eras but clamps each to the window edges:
    // 2008..2010 + 2015..2017.
    expect(rowAddPeriod(disjoint, yr(2008, 2017))).toBe(
      "2008..2010,2015..2017",
    );
    // A window inside the GAP keeps neither era → fall back to the row's own wire.
    expect(rowAddPeriod(disjoint, yr(2012, 2013))).toBe(
      "2005..2010,2015..2020",
    );
    // A window over only the first era keeps just it.
    expect(rowAddPeriod(disjoint, yr(2006, 2009))).toBe("2006..2009");
  });
});

describe("addWindowBounds (#678 finding 1: sub-annual period honored on add)", () => {
  it("a sub-annual ?period resolves to its EXACT ISO bounds (not the outer year)", () => {
    // The whole point of the finding: 2020-Q1 must NOT collapse to 2020.
    expect(addWindowBounds("2020-Q1", [2020, 2020])).toEqual({
      from: "2020-01-01",
      to: "2020-03-31",
    });
    expect(addWindowBounds("HT2020", [2020, 2020])).toEqual({
      from: "2020-07-01",
      to: "2020-12-31",
    });
    expect(addWindowBounds("2020-03", [2020, 2020])).toEqual({
      from: "2020-03-01",
      to: "2020-03-31",
    });
  });

  it("a year ?period resolves to the whole-year ISO bounds", () => {
    expect(addWindowBounds("2018", [2018, 2018])).toEqual({
      from: "2018-01-01",
      to: "2018-12-31",
    });
  });

  it("a ?period that parses to no bound (_default) falls back to the year window", () => {
    expect(addWindowBounds("_default", [2000, 2004])).toEqual({
      from: "2000-01-01",
      to: "2004-12-31",
    });
  });

  it("with no ?period it expands the year window to ISO bounds; null with neither", () => {
    expect(addWindowBounds(null, [2010, 2015])).toEqual({
      from: "2010-01-01",
      to: "2015-12-31",
    });
    expect(addWindowBounds(null, null)).toBeNull();
  });
});

describe("commonLabelStem + labelSuffix (#678 value-set stem dedup)", () => {
  it("hoists the majority word-stem when most labels share a long leading sequence", () => {
    // sni92: most rows repeat "Svensk standard för näringsgrensindelning," and differ
    // only in the suffix; a couple of outliers vary. The shared stem is hoisted.
    const stem = commonLabelStem([
      "Svensk standard för näringsgrensindelning, SNI 92, Aktiviteter",
      "Svensk standard för näringsgrensindelning, brancher SNI 92",
      "Svensk standard för näringsgrensindelning, SNI 92 detaljgrupp",
      "SNI, 92, detaljgrupp", // outlier — does not share the stem
    ]);
    expect(stem).toBe("Svensk standard för näringsgrensindelning,");
  });

  it("tolerates a single outlier (majority, not all)", () => {
    const stem = commonLabelStem([
      "Standard för svensk näringsgren Branscher",
      "Standard för svensk näringsgren Aktiviteter",
      "Helt annan etikett här borta", // 1 of 3 — majority is 2
    ]);
    expect(stem).toBe("Standard för svensk näringsgren");
  });

  it("returns '' when no substantial majority stem exists (< 2 words or < 10 chars)", () => {
    // "SNI 92" is a 2-word shared lead but only 6 chars → not substantial.
    expect(commonLabelStem(["SNI 92 A", "SNI 92 B"])).toBe("");
    // No shared leading word at all.
    expect(commonLabelStem(["Alpha beta gamma", "Zeta eta theta"])).toBe("");
    // A single shared word is < 2 words.
    expect(
      commonLabelStem(["Inkomst total brutto", "Inkomst netto skatt"]),
    ).toBe("");
  });

  it("empty input / all-empty labels → ''", () => {
    expect(commonLabelStem([])).toBe("");
    expect(commonLabelStem(["", ""])).toBe("");
  });

  it("labelSuffix strips the stem + leading separators, keeps outliers full", () => {
    const stem = "Svensk standard för näringsgrensindelning,";
    expect(
      labelSuffix("Svensk standard för näringsgrensindelning, SNI 92", stem),
    ).toBe("SNI 92");
    // An outlier (doesn't start with the stem) keeps its full label.
    expect(labelSuffix("SNI, 92, detaljgrupp", stem)).toBe(
      "SNI, 92, detaljgrupp",
    );
    // A label that IS exactly the stem → "" (nothing left for the row).
    expect(labelSuffix(stem, stem)).toBe("");
    // No stem → unchanged.
    expect(labelSuffix("anything", "")).toBe("anything");
  });
});

describe("pickerLabeling (#678 1b adaptive labels)", () => {
  // A representation row, in the shape pickerLabeling consumes (only the four
  // labeled dimensions + the selection key matter).
  function rep(over: Partial<PickerRepresentation>): PickerRepresentation {
    // variantLabel defaults to the slug (the NULL-named fallback), so a test that
    // overrides only `variant` still labels by that slug unless it sets variantLabel.
    return {
      key: `${over.variant ?? "v"}::${over.column ?? "Col"}`,
      variant: "v",
      variantLabel: over.variant ?? "v",
      column: "Col",
      representation: over.column ?? "Col",
      from: "2000-01-01",
      to: "2010-12-31",
      windows: [{ from: "2000-01-01", to: "2010-12-31" }],
      period: "2000 – 2010",
      wirePeriod: "2000..2010",
      valueSetLabel: "",
      codingsVary: false,
      renamedColumns: [],
      ...over,
    };
  }

  it("column varies → column is the mono primary; a constant variant is NOT hoisted", () => {
    const { headerContext, rows } = pickerLabeling([
      rep({ variant: "v1", column: "Ssyk3", valueSetLabel: "SSYK 3" }),
      rep({ variant: "v1", column: "Ssyk4", valueSetLabel: "SSYK 4" }),
    ]);
    // A constant variant ("v1") is dropped as noise (#678: single-register default,
    // already in the add coordinate); column varies → on the rows.
    expect(headerContext).not.toContain("v1");
    expect(headerContext).not.toContain("column Ssyk3");
    expect(rows[0].primary).toEqual({ text: "Ssyk3", mono: true });
    expect(rows[1].primary).toEqual({ text: "Ssyk4", mono: true });
  });

  it("fordonsreg shape: only the population varies → population primary, constant column + value set hoisted (no period, no variant)", () => {
    // Every row delivers the constant column "Sni2002"; the POPULATION
    // (lastbilar/bussar) is what distinguishes them.
    const { column, headerContext, rows } = pickerLabeling([
      rep({
        variant: "lastbilar",
        column: "Sni2002",
        valueSetLabel: "SNI 2002",
      }),
      rep({ variant: "bussar", column: "Sni2002", valueSetLabel: "SNI 2002" }),
    ]);
    // The constant column hoists to the dedicated `column` chip field; the constant
    // value set hoists to the quiet context; the period is NEVER hoisted and the
    // (varying) variant stays on rows.
    expect(column).toBe("Sni2002");
    expect(headerContext).toEqual(["SNI 2002"]);
    // Each row shows the varying population as the (non-mono) primary.
    expect(rows.map((r) => r.primary)).toEqual([
      { text: "lastbilar", mono: false },
      { text: "bussar", mono: false },
    ]);
    expect(rows.every((r) => r.qualifiers.length === 0)).toBe(true);
  });

  it("displays the variant LABEL (name), not the slug, when the variant is the primary (#793)", () => {
    // The variant varies → it's the row primary. The DISPLAYED text is variantLabel
    // (the curator name), even though variance is keyed on the slug.
    const { rows } = pickerLabeling([
      rep({
        variant: "snoskotrar",
        variantLabel: "Snöskotrar",
        column: "Sni",
        valueSetLabel: "SNI 2002",
      }),
      rep({
        variant: "slapvagnar",
        variantLabel: "Släpvagnar",
        column: "Sni",
        valueSetLabel: "SNI 2002",
      }),
    ]);
    expect(rows.map((r) => r.primary.text)).toEqual([
      "Snöskotrar",
      "Släpvagnar",
    ]);
  });

  it("fordonsreg empty-label shape: a value-set label constant except on empty rows still hoists (#678 fix 3)", () => {
    // One population delivers no value set (empty label). The single real label must
    // read as CONSTANT and hoist — not show per-row — and the empty row shows nothing.
    const { column, headerContext, rows } = pickerLabeling([
      rep({
        variant: "lastbilar",
        column: "Sni2002",
        valueSetLabel: "",
      }),
      rep({
        variant: "bussar",
        column: "Sni2002",
        valueSetLabel:
          "Standard för svensk näringsgrensindelning, 2002 Branscher",
      }),
    ]);
    expect(column).toBe("Sni2002");
    expect(headerContext).toEqual([
      "Standard för svensk näringsgrensindelning, 2002 Branscher",
    ]);
    // The value set is treated as constant → it is NOT a per-row varying qualifier.
    expect(rows.every((r) => r.qualifiers.length === 0)).toBe(true);
    expect(rows.map((r) => r.primary)).toEqual([
      { text: "lastbilar", mono: false },
      { text: "bussar", mono: false },
    ]);
  });

  it("yrkesreg shape: column + population + value set vary, nothing constant hoists (period never, variant varies)", () => {
    const { headerContext, rows } = pickerLabeling([
      rep({
        variant: "anställda",
        column: "Sun2020Niva",
        valueSetLabel: "SUN 2020 nivå",
        period: "2020 – 2023",
      }),
      rep({
        variant: "egenföretagare",
        column: "Sun2020Inr",
        valueSetLabel: "SUN 2020 inriktning",
        period: "2020 – 2023",
      }),
    ]);
    // The constant period is NEVER hoisted; the three varying dims stay on rows → no
    // header context at all.
    expect(headerContext).toEqual([]);
    // Priority: column (mono) is primary, then variant + value set as qualifiers.
    expect(rows[0].primary).toEqual({ text: "Sun2020Niva", mono: true });
    expect(rows[0].qualifiers).toEqual(["anställda", "SUN 2020 nivå"]);
    // The period is not on the rows here (it's constant; the picker's right-side
    // period column shows it from `row.period` directly, not this label).
    expect(rows.every((r) => r.period === null)).toBe(true);
  });

  it("sni92 stem dedup: value-set labels share a long stem → hoist it, rows show suffixes, outlier full (#678)", () => {
    // Labels VARY (so no identical-all collapse) but share a long majority stem. The
    // stem hoists to the context once; each sharing row shows only its suffix; the
    // outlier (no shared stem) keeps its full label. The varying COLUMN is the primary.
    const { headerContext, rows } = pickerLabeling([
      rep({
        variant: "v",
        column: "Sni92A",
        valueSetLabel: "Svensk standard för näringsgrensindelning, Aktiviteter",
      }),
      rep({
        variant: "v",
        column: "Sni92B",
        valueSetLabel: "Svensk standard för näringsgrensindelning, Branscher",
      }),
      rep({
        variant: "v",
        column: "Sni92C",
        valueSetLabel: "SNI 92 detaljgrupp", // outlier — no shared stem
      }),
    ]);
    // The stem hoists to the quiet context (column is the primary, not hoisted).
    expect(headerContext).toEqual([
      "Svensk standard för näringsgrensindelning,",
    ]);
    // Sharing rows show only their suffix as the qualifier; the outlier keeps full.
    expect(rows.map((r) => r.qualifiers)).toEqual([
      ["Aktiviteter"],
      ["Branscher"],
      ["SNI 92 detaljgrupp"],
    ]);
    // The column is each row's mono primary.
    expect(rows.map((r) => r.primary.text)).toEqual([
      "Sni92A",
      "Sni92B",
      "Sni92C",
    ]);
  });

  it("identical-all value-set labels still collapse (the stem covers the whole label)", () => {
    // When every label is identical, the identical-all hoist still applies (no stem
    // path needed) — the label hoists to context, rows carry no value-set qualifier.
    const { headerContext, rows } = pickerLabeling([
      rep({ variant: "v", column: "A", valueSetLabel: "SUN 2020" }),
      rep({ variant: "v", column: "B", valueSetLabel: "SUN 2020" }),
    ]);
    expect(headerContext).toEqual(["SUN 2020"]);
    expect(rows.every((r) => r.qualifiers.length === 0)).toBe(true);
  });

  it("no substantial stem → full value-set labels per row (no hoist)", () => {
    const { headerContext, rows } = pickerLabeling([
      rep({ variant: "v", column: "A", valueSetLabel: "Alpha one" }),
      rep({ variant: "v", column: "B", valueSetLabel: "Beta two" }),
    ]);
    // No shared stem → nothing hoisted; each row shows its full label.
    expect(headerContext).toEqual([]);
    expect(rows.map((r) => r.qualifiers)).toEqual([
      ["Alpha one"],
      ["Beta two"],
    ]);
  });

  it("period varies → it rides on the row, not the header", () => {
    const { headerContext, rows } = pickerLabeling([
      rep({ variant: "v1", column: "A", period: "2000 – 2005" }),
      rep({ variant: "v1", column: "B", period: "2006 – 2010" }),
    ]);
    expect(headerContext).not.toContain("2000 – 2005");
    expect(rows.map((r) => r.period)).toEqual(["2000 – 2005", "2006 – 2010"]);
  });

  it("a single representation (nothing varies) → the column is the mono primary; column field + value set hoist, no variant/period", () => {
    const { column, headerContext, rows } = pickerLabeling([
      rep({ variant: "v1", column: "Kon", valueSetLabel: "1-siffrig" }),
    ]);
    // The lone row never renders blank: its column is the identifier.
    expect(rows[0].primary).toEqual({ text: "Kon", mono: true });
    expect(rows[0].qualifiers).toEqual([]);
    // The constant column hoists to the `column` field; the value set to the quiet
    // context; the constant variant is NOT hoisted (noise) and the period never is.
    // (In the PICKER, a single-column variable's `column` is suppressed from context
    // since the column becomes the row's primary chip — that's the picker's `view`.)
    expect(column).toBe("Kon");
    expect(headerContext).toEqual(["1-siffrig"]);
  });

  it("falls back to the variant, then a dash, when no column is present", () => {
    // A degenerate single row with no column (shouldn't occur — enumeration skips
    // null columns — but the fallback must never render blank).
    const [withVariant] = pickerLabeling([
      rep({ variant: "only-pop", column: "" }),
    ]).rows;
    expect(withVariant.primary).toEqual({ text: "only-pop", mono: false });
    const [bare] = pickerLabeling([rep({ variant: "", column: "" })]).rows;
    expect(bare.primary).toEqual({ text: "—", mono: false });
  });
});

describe("pickerFilterDimensions / pickerRowPasses (#908)", () => {
  function row(over: Partial<PickerRepresentation>): PickerRepresentation {
    return {
      key: `${over.variant ?? "v"}::${over.column ?? "Col"}`,
      variant: "v",
      variantLabel: over.variant ?? "v",
      column: over.column ?? "Col",
      representation: over.column ?? "Col",
      from: "2000-01-01",
      to: "2010-12-31",
      windows: [{ from: "2000-01-01", to: "2010-12-31" }],
      period: "2000 – 2010",
      wirePeriod: "2000..2010",
      valueSetLabel: "",
      codingsVary: false,
      renamedColumns: [],
      ...over,
    };
  }
  // A band carrying a single representation column with the given facets on that column.
  function fband(
    column: string,
    facets: { axis: string; value: string; label: string }[],
    rowOver: Partial<PickerRepresentation> = {},
  ) {
    return {
      rows: [row({ column, ...rowOver })],
      facetsByColumn: { [column]: facets },
    };
  }
  const axes = [
    { name: "enhet", label: "Enhet" },
    { name: "hush", label: "Hushållsbegrepp" },
  ];

  it("emits a facet axis only when it discriminates (≥2 distinct values)", () => {
    const bands = [
      fband("DIN1", [
        { axis: "enhet", value: "ind", label: "Individ" },
        { axis: "hush", value: "h1", label: "Hushåll A" },
      ]),
      fband("DIN2", [
        { axis: "enhet", value: "ind", label: "Individ" },
        { axis: "hush", value: "h2", label: "Hushåll B" },
      ]),
    ];
    const dims = pickerFilterDimensions(bands, axes);
    // `enhet` is constant (all "ind") → not a filter; `hush` varies → a filter.
    // The facet key is namespaced (#908 C2); the raw axis name rides on `axis`.
    expect(dims.map((d) => d.key)).toEqual(["facet:hush"]);
    const hush = dims[0];
    expect(hush.kind).toBe("facet");
    expect(hush.axis).toBe("hush");
    expect(hush.label).toBe("Hushållsbegrepp");
    expect(hush.values).toEqual([
      { value: "h1", label: "Hushåll A" },
      { value: "h2", label: "Hushåll B" },
    ]);
  });

  it("surfaces variant + coding dimensions when they discriminate; never single-value", () => {
    const bands = [
      {
        rows: [row({ variant: "ind", column: "C", valueSetLabel: "SNI 2002" })],
      },
      {
        rows: [row({ variant: "fam", column: "C", valueSetLabel: "SNI 2007" })],
      },
    ];
    const dims = pickerFilterDimensions(bands, []);
    expect(dims.map((d) => d.kind)).toEqual(["variant", "coding"]);
    expect(dims[0].label).toBe("Population");
    expect(dims[0].values.map((v) => v.value).sort()).toEqual(["fam", "ind"]);
    expect(dims[1].label).toBe("Coding");
    expect(dims[1].values.map((v) => v.value)).toEqual([
      "SNI 2002",
      "SNI 2007",
    ]);
  });

  it("a single-population, single-coding, single-axis group surfaces NO dimension", () => {
    const bands = [
      fband("C", [{ axis: "enhet", value: "ind", label: "Individ" }], {
        valueSetLabel: "SNI",
      }),
    ];
    expect(pickerFilterDimensions(bands, axes)).toEqual([]);
  });

  it("an empty value-set label contributes no coding value", () => {
    const bands = [
      { rows: [row({ column: "A", valueSetLabel: "X" })] },
      { rows: [row({ column: "B", valueSetLabel: "" })] },
    ];
    // Only one non-empty coding label → not a filter.
    expect(
      pickerFilterDimensions(bands, []).some((d) => d.kind === "coding"),
    ).toBe(false);
  });

  it("pickerRowPasses: AND across dimensions, OR within a dimension", () => {
    const dims: PickerDimension[] = [
      {
        kind: "facet",
        key: "hush",
        label: "Hushållsbegrepp",
        values: [
          { value: "h1", label: "A" },
          { value: "h2", label: "B" },
        ],
      },
      {
        kind: "variant",
        key: "variant",
        label: "Population",
        values: [
          { value: "ind", label: "ind" },
          { value: "fam", label: "fam" },
        ],
      },
    ];
    const band = fband("DIN1", [{ axis: "hush", value: "h1", label: "A" }], {
      variant: "ind",
    });
    const theRow = band.rows[0];
    // No selection → passes.
    expect(pickerRowPasses(theRow, band, dims, {})).toBe(true);
    // Matching facet → passes.
    expect(pickerRowPasses(theRow, band, dims, { hush: new Set(["h1"]) })).toBe(
      true,
    );
    // Non-matching facet → fails.
    expect(pickerRowPasses(theRow, band, dims, { hush: new Set(["h2"]) })).toBe(
      false,
    );
    // OR within: either value selected passes.
    expect(
      pickerRowPasses(theRow, band, dims, { hush: new Set(["h1", "h2"]) }),
    ).toBe(true);
    // AND across: facet matches but variant doesn't → fails.
    expect(
      pickerRowPasses(theRow, band, dims, {
        hush: new Set(["h1"]),
        variant: new Set(["fam"]),
      }),
    ).toBe(false);
  });

  it("pickerRowPasses: a row lacking a facet on a SELECTED axis fails that axis", () => {
    const dims: PickerDimension[] = [
      {
        kind: "facet",
        key: "hush",
        label: "Hushållsbegrepp",
        values: [{ value: "h1", label: "A" }],
      },
    ];
    // The band carries no facet on `hush` for this column.
    const band = { rows: [row({ column: "C" })], facetsByColumn: {} };
    expect(
      pickerRowPasses(band.rows[0], band, dims, { hush: new Set(["h1"]) }),
    ).toBe(false);
  });

  it("pickerRowPasses: coding branch matches the row's value-set label; code-less always fails", () => {
    const dims: PickerDimension[] = [
      {
        kind: "coding",
        key: "coding",
        label: "Coding",
        values: [
          { value: "SNI 2002", label: "SNI 2002" },
          { value: "SNI 2007", label: "SNI 2007" },
        ],
      },
    ];
    const band = { rows: [row({ column: "C", valueSetLabel: "SNI 2002" })] };
    const coded = band.rows[0];
    // In the selected coding set → passes; not in it → fails.
    expect(
      pickerRowPasses(coded, band, dims, { coding: new Set(["SNI 2002"]) }),
    ).toBe(true);
    expect(
      pickerRowPasses(coded, band, dims, { coding: new Set(["SNI 2007"]) }),
    ).toBe(false);
    // A code-less row (valueSetLabel "") is NOT a coding choice — it fails ANY
    // active coding filter, even one whose set is non-empty (intended design).
    const bare = row({ column: "D", valueSetLabel: "" });
    expect(
      pickerRowPasses(bare, { rows: [bare] }, dims, {
        coding: new Set(["SNI 2002", "SNI 2007"]),
      }),
    ).toBe(false);
  });

  it("pickerFilterDimensions: dedupes a facet axis across a band's sibling columns (#819 group shape)", () => {
    // One band / fqid carrying several sibling delivery columns (the representation-
    // group shape): the `hush` axis takes value h1 on two columns and h2 on a third.
    // The inner per-row loop must dedupe h1 across the two columns to a single value.
    const band = {
      rows: [
        row({ column: "C1" }),
        row({ column: "C2" }),
        row({ column: "C3" }),
      ],
      facetsByColumn: {
        C1: [{ axis: "hush", value: "h1", label: "Hushåll A" }],
        C2: [{ axis: "hush", value: "h1", label: "Hushåll A" }],
        C3: [{ axis: "hush", value: "h2", label: "Hushåll B" }],
      },
    };
    const dims = pickerFilterDimensions([band], axes);
    expect(dims.map((d) => d.key)).toEqual(["facet:hush"]);
    // h1 appears on two columns but dedupes to one value alongside h2.
    expect(dims[0].values).toEqual([
      { value: "h1", label: "Hushåll A" },
      { value: "h2", label: "Hushåll B" },
    ]);
  });

  // ── C1: band-level facets (whole-variable faceted members) ────────────────
  // A whole-variable faceted member has a null delivery_column, so its facets are
  // carried band-level (`band.facets`) and apply to EVERY row, regardless of column.
  it("rowFacet falls back to band-level facets when there's no per-column facet (C1)", () => {
    const band = {
      rows: [row({ column: "C1" }), row({ column: "C2" })],
      facets: [{ axis: "month", value: "01", label: "January" }],
    };
    // No `facetsByColumn` → the band-level facet applies to any row's column.
    expect(rowFacet(band, band.rows[0], "month")?.value).toBe("01");
    expect(rowFacet(band, band.rows[1], "month")?.value).toBe("01");
    expect(rowFacet(band, band.rows[0], "missing")).toBeUndefined();
  });

  it("rowFacet prefers the per-column facet over the band-level one (C1)", () => {
    const band = {
      rows: [row({ column: "C1" }), row({ column: "C2" })],
      facetsByColumn: {
        C1: [{ axis: "month", value: "02", label: "February" }],
      },
      facets: [{ axis: "month", value: "01", label: "January" }],
    };
    // C1 has a per-column facet → it wins; C2 has none → falls back to band-level.
    expect(rowFacet(band, band.rows[0], "month")?.value).toBe("02");
    expect(rowFacet(band, band.rows[1], "month")?.value).toBe("01");
  });

  it("pickerFilterDimensions surfaces a band-level axis across rows (C1)", () => {
    // Two whole-variable faceted bands, each a different month → the axis discriminates.
    const bands = [
      {
        rows: [row({ column: "JAN" })],
        facets: [{ axis: "month", value: "01", label: "January" }],
      },
      {
        rows: [row({ column: "FEB" })],
        facets: [{ axis: "month", value: "02", label: "February" }],
      },
    ];
    const dims = pickerFilterDimensions(bands, [
      { name: "month", label: "Month" },
    ]);
    expect(dims.map((d) => d.key)).toEqual(["facet:month"]);
    expect(dims[0].axis).toBe("month");
    expect(dims[0].values).toEqual([
      { value: "01", label: "January" },
      { value: "02", label: "February" },
    ]);
  });

  it("pickerRowPasses filters by a band-level facet (C1)", () => {
    const dims = pickerFilterDimensions(
      [
        {
          rows: [row({ column: "JAN" })],
          facets: [{ axis: "month", value: "01", label: "January" }],
        },
        {
          rows: [row({ column: "FEB" })],
          facets: [{ axis: "month", value: "02", label: "February" }],
        },
      ],
      [{ name: "month", label: "Month" }],
    );
    const janBand = {
      rows: [row({ column: "JAN" })],
      facets: [{ axis: "month", value: "01", label: "January" }],
    };
    const janRow = janBand.rows[0];
    // The namespaced selection key (`facet:month`) is what pickerRowPasses reads.
    expect(
      pickerRowPasses(janRow, janBand, dims, {
        "facet:month": new Set(["01"]),
      }),
    ).toBe(true);
    expect(
      pickerRowPasses(janRow, janBand, dims, {
        "facet:month": new Set(["02"]),
      }),
    ).toBe(false);
  });

  // ── C2: facet key namespacing vs. built-in dimension keys ──────────────────
  it("namespaces a facet key so an axis named 'coding' can't collide with the built-in coding dim (C2)", () => {
    // A declared axis literally named "coding", AND rows that also vary on the
    // built-in coding (value-set label). Both must surface as DISTINCT dimensions.
    const bands: PickerBandFacets[] = [
      {
        rows: [row({ column: "A", valueSetLabel: "SNI 2002" })],
        facetsByColumn: { A: [{ axis: "coding", value: "x", label: "X" }] },
      },
      {
        rows: [row({ column: "B", valueSetLabel: "SNI 2007" })],
        facetsByColumn: { B: [{ axis: "coding", value: "y", label: "Y" }] },
      },
    ];
    const dims = pickerFilterDimensions(bands, [
      { name: "coding", label: "Coding axis" },
    ]);
    // Two distinct dimensions: the facet (namespaced) and the built-in coding.
    const facetDim = dims.find((d) => d.kind === "facet");
    const codingDim = dims.find((d) => d.kind === "coding");
    expect(facetDim?.key).toBe("facet:coding");
    expect(facetDim?.axis).toBe("coding");
    expect(codingDim?.key).toBe("coding");
    expect(codingDim?.axis).toBeUndefined();
    // Distinct keys → no duplicate Svelte #each key, no shared selection slot.
    expect(new Set(dims.map((d) => d.key)).size).toBe(dims.length);
  });

  it("a selection on the facet axis 'coding' does not bleed into the built-in coding dim (C2)", () => {
    const bands: PickerBandFacets[] = [
      {
        rows: [row({ column: "A", valueSetLabel: "SNI 2002" })],
        facetsByColumn: { A: [{ axis: "coding", value: "x", label: "X" }] },
      },
      {
        rows: [row({ column: "B", valueSetLabel: "SNI 2007" })],
        facetsByColumn: { B: [{ axis: "coding", value: "y", label: "Y" }] },
      },
    ];
    const dims = pickerFilterDimensions(bands, [
      { name: "coding", label: "Coding axis" },
    ]);
    const bandA = bands[0];
    const rowA = bandA.rows[0]; // facet coding=x, value-set "SNI 2002"
    // Select the FACET value "x" only — the built-in coding dim has no selection, so
    // it imposes no constraint; rowA passes (its facet IS "x").
    expect(
      pickerRowPasses(rowA, bandA, dims, { "facet:coding": new Set(["x"]) }),
    ).toBe(true);
    // Select the built-in CODING value "SNI 2007" only — rowA's value-set is
    // "SNI 2002", so it fails. The facet selection slot ("facet:coding") is separate
    // and untouched, proving no bleed: the same literal "coding" lives in two slots.
    expect(
      pickerRowPasses(rowA, bandA, dims, { coding: new Set(["SNI 2007"]) }),
    ).toBe(false);
    // And selecting the facet "x" must NOT satisfy a built-in coding filter for a
    // different value-set: distinct slots, no cross-talk.
    expect(
      pickerRowPasses(rowA, bandA, dims, {
        "facet:coding": new Set(["x"]),
        coding: new Set(["SNI 2007"]),
      }),
    ).toBe(false);
  });
});

describe("bandLabeling (#678 inc 2 adaptive band identity)", () => {
  const band = (
    over: Partial<{
      name: string;
      registerPrefix: string;
      facetLabel: string | null;
      distinguisher: string;
      distinguisherIsColumn: boolean;
    }> = {},
  ) => ({
    name: "Näringsgren",
    registerPrefix: "scb/moms",
    facetLabel: null,
    distinguisher: "Ng0",
    // The default distinguisher is a delivery column (a single-column member).
    distinguisherIsColumn: true,
    ...over,
  });

  it("a single-COLUMN leaf leads with its COLUMN, not the (repeated) name", () => {
    const { showName, showPrefix, bands } = bandLabeling([
      band({ name: "Kön", registerPrefix: "scb/lisa", distinguisher: "Kon" }),
    ]);
    // One band, one column → the variable name is already the page <h2>, so the leaf
    // leads with just its column chip (`Kon`), not a repeated `Kön`. Both constant dims
    // are hoisted off (the title / breadcrumb carry them).
    expect(bands[0].primary).toEqual({ text: "Kon", mono: true });
    expect(bands[0].primaryIsColumn).toBe(true);
    expect(bands[0].primaryIsFacet).toBe(false);
    expect(showName).toBe(false);
    expect(showPrefix).toBe(false);
  });

  it("a single-band MULTI-column leaf leads with the variable NAME", () => {
    // A lone band whose columns are the rows beneath has no single column to lead
    // with → the variable name is the right umbrella identity.
    const { bands } = bandLabeling([
      band({
        name: "Yrke",
        distinguisher: "yrkesreg",
        distinguisherIsColumn: false,
      }),
    ]);
    expect(bands[0].primary).toEqual({ text: "Yrke", mono: false });
    expect(bands[0].primaryIsColumn).toBe(false);
    expect(bands[0].primaryIsFacet).toBe(false);
  });

  it("a name-constant group leads each band with its distinguishing COLUMN (mono)", () => {
    // The moms/naringsgren shape: every member is "Näringsgren" on `scb/moms`, only
    // the delivery column varies → drop the repeated name + prefix, lead with Ng0/…
    const { showName, showPrefix, bands } = bandLabeling([
      band({ distinguisher: "Ng0" }),
      band({ distinguisher: "Ng1" }),
      band({ distinguisher: "Sni" }),
    ]);
    expect(bands.map((b) => b.primary.text)).toEqual(["Ng0", "Ng1", "Sni"]);
    expect(bands.every((b) => b.primary.mono)).toBe(true);
    expect(bands.every((b) => b.primaryIsColumn)).toBe(true);
    expect(bands.every((b) => b.primaryIsFacet)).toBe(false);
    // Name + prefix are constant → hoisted off every band.
    expect(showName).toBe(false);
    expect(showPrefix).toBe(false);
  });

  it("a multi-column member leads with its SLUG (mono), NOT marked a column", () => {
    // A genuinely multi-column member has no single delivery column to lead with, so
    // the distinguisher is its leaf slug (`distinguisherIsColumn: false`). It still
    // leads mono (a technical identifier) but is NOT the column chip-link identity —
    // the band renders a plain slug + per-row column chips.
    const { bands } = bandLabeling([
      band({ distinguisher: "sun2020niva", distinguisherIsColumn: false }),
      band({ distinguisher: "Ng1" }),
    ]);
    expect(bands[0].primary).toEqual({ text: "sun2020niva", mono: true });
    expect(bands[0].primaryIsColumn).toBe(false);
    expect(bands[0].primaryIsFacet).toBe(false);
    // Its single-column sibling still leads with its column chip identity.
    expect(bands[1].primaryIsColumn).toBe(true);
    expect(bands[1].primaryIsFacet).toBe(false);
  });

  it("a facet group leads each band with its FACET label (normal weight)", () => {
    // The moderns-utbildningsniva shape: name constant, a facet axis varies → the
    // facet (e.g. specialskola) leads, NOT the column.
    const { bands } = bandLabeling([
      band({ facetLabel: "specialskola", distinguisher: "Ng0" }),
      band({ facetLabel: "grundskola", distinguisher: "Ng1" }),
    ]);
    expect(bands.map((b) => b.primary.text)).toEqual([
      "specialskola",
      "grundskola",
    ]);
    expect(bands.every((b) => b.primary.mono)).toBe(false);
    expect(bands.every((b) => b.primaryIsColumn)).toBe(false);
    // The primary IS the facet → `primaryIsFacet` true, so the row suppresses the
    // redundant facet repeat in its `.sub` context (#901).
    expect(bands.every((b) => b.primaryIsFacet)).toBe(true);
  });

  it("genuinely different concepts lead with the NAME and keep it visible", () => {
    const { showName, bands } = bandLabeling([
      band({ name: "Inkomst", distinguisher: "Ink" }),
      band({ name: "Ålder", distinguisher: "Age" }),
    ]);
    expect(bands.map((b) => b.primary.text)).toEqual(["Inkomst", "Ålder"]);
    expect(bands.every((b) => b.primary.mono)).toBe(false);
    expect(bands.every((b) => b.primaryIsFacet)).toBe(false);
    // The name varies → it IS the distinguisher, so it stays as the primary (not
    // double-shown as secondary; `showNameSecondary` in the band guards the echo).
    expect(showName).toBe(true);
  });

  it("hoists only the constant dimension when name varies but prefix is constant", () => {
    const { showName, showPrefix } = bandLabeling([
      band({ name: "Inkomst", registerPrefix: "scb/lisa" }),
      band({ name: "Ålder", registerPrefix: "scb/lisa" }),
    ]);
    expect(showName).toBe(true);
    expect(showPrefix).toBe(false);
  });
});

describe("clusterBands (#901 de-duplicate member presentation)", () => {
  const band = (
    over: Partial<{
      name: string;
      registerPrefix: string;
      facetLabel: string | null;
      distinguisher: string;
      distinguisherIsColumn: boolean;
    }> = {},
  ) => ({
    name: "Disponibel inkomst",
    registerPrefix: "scb/iot",
    facetLabel: null,
    distinguisher: "CDISP",
    distinguisherIsColumn: true,
    ...over,
  });
  const id = (b: ReturnType<typeof band>) => b;

  it("a name-CONSTANT group is ONE cluster with no headings (today's behavior)", () => {
    // The homogeneous moms/naringsgren shape: every member shares the name → one
    // cluster, so `showClusterHeadings` is false (the name is already the page <h2>)
    // and the cluster's own labeling leads each band with its column distinguisher.
    const { clusters, showClusterHeadings } = clusterBands(
      [band({ distinguisher: "Ng0" }), band({ distinguisher: "Ng1" })],
      id,
    );
    expect(showClusterHeadings).toBe(false);
    expect(clusters).toHaveLength(1);
    expect(clusters[0].name).toBe("Disponibel inkomst");
    // Inside the cluster the name is constant → bands lead with the column.
    expect(clusters[0].labeling.bands.map((b) => b.primary.text)).toEqual([
      "Ng0",
      "Ng1",
    ]);
    expect(clusters[0].labeling.showName).toBe(false);
  });

  it("a single band is ONE cluster with no heading (the leaf)", () => {
    const { clusters, showClusterHeadings } = clusterBands([band()], id);
    expect(showClusterHeadings).toBe(false);
    expect(clusters).toHaveLength(1);
  });

  it("a heterogeneous group clusters by name and shows headings", () => {
    // The #901 disponibel-inkomst shape: several distinct names, each repeated. Group
    // by name → one cluster per distinct name (headings shown), and WITHIN each
    // cluster the (now constant) name is hoisted so the band leads with its column.
    const { clusters, showClusterHeadings } = clusterBands(
      [
        band({ name: "Disponibel inkomst", distinguisher: "CDISP04HB" }),
        band({ name: "Disponibel inkomst, familj", distinguisher: "DINF" }),
        band({ name: "Disponibel inkomst", distinguisher: "CDISPHB" }),
        band({ name: "Disponibel inkomst, familj", distinguisher: "DINKF" }),
      ],
      id,
    );
    expect(showClusterHeadings).toBe(true);
    // Clusters appear in first-seen name order; bands keep input order within.
    expect(clusters.map((c) => c.name)).toEqual([
      "Disponibel inkomst",
      "Disponibel inkomst, familj",
    ]);
    expect(clusters[0].bands.map((b) => b.distinguisher)).toEqual([
      "CDISP04HB",
      "CDISPHB",
    ]);
    expect(clusters[1].bands.map((b) => b.distinguisher)).toEqual([
      "DINF",
      "DINKF",
    ]);
    // Each cluster's name is constant → its bands lead with their distinguishing
    // COLUMN, the name hoisted off (showName false) to the heading.
    expect(clusters[0].labeling.showName).toBe(false);
    expect(clusters[0].labeling.bands.map((b) => b.primary.text)).toEqual([
      "CDISP04HB",
      "CDISPHB",
    ]);
    expect(clusters[1].labeling.bands.map((b) => b.primary.text)).toEqual([
      "DINF",
      "DINKF",
    ]);
  });

  it("a singleton-name member still earns its own heading", () => {
    // A name that appears once is its own cluster — with several distinct names the
    // group still shows headings, so that singleton renders its name once uniformly.
    const { clusters, showClusterHeadings } = clusterBands(
      [
        band({ name: "Disponibel inkomst", distinguisher: "CDISP" }),
        band({ name: "Delkomponent", distinguisher: "DIND" }),
      ],
      id,
    );
    expect(showClusterHeadings).toBe(true);
    expect(clusters.map((c) => c.name)).toEqual([
      "Disponibel inkomst",
      "Delkomponent",
    ]);
    expect(clusters.every((c) => c.bands.length === 1)).toBe(true);
  });

  it("a lone multi-column band in a heading group leads with its distinguisher, not the (heading) name (#901)", () => {
    // The disponibel-inkomst regression: a heterogeneous group (headings shown) where
    // one name maps to a SINGLE multi-column band (`distinguisherIsColumn: false`).
    // `bandLabeling([band])` falls through to the lone-band name fallback and would lead
    // it with `band.name` — but that name is ALSO the cluster heading, so it would show
    // twice. clusterBands re-leads that band with its slug distinguisher (mono, not a
    // column), so the name appears ONLY in the heading.
    const { clusters, showClusterHeadings } = clusterBands(
      [
        band({
          name: "Delkomponent, 2004 års definition",
          distinguisher: "delkomponent-2004",
          distinguisherIsColumn: false,
        }),
        band({ name: "Disponibel inkomst", distinguisher: "CDISP" }),
      ],
      id,
    );
    expect(showClusterHeadings).toBe(true);
    const lone = clusters[0];
    expect(lone.name).toBe("Delkomponent, 2004 års definition");
    expect(lone.bands).toHaveLength(1);
    // Leads with the slug distinguisher (NOT the name), as a mono non-column primary.
    expect(lone.labeling.bands[0].primary.text).toBe("delkomponent-2004");
    expect(lone.labeling.bands[0].primary.mono).toBe(true);
    expect(lone.labeling.bands[0].primaryIsColumn).toBe(false);
    // The re-led primary is the slug distinguisher, not a facet.
    expect(lone.labeling.bands[0].primaryIsFacet).toBe(false);
  });

  it("a single-CLUSTER group does NOT re-lead a lone band (the name/column still leads)", () => {
    // Guard the regression: with ONE cluster (no headings) the lone leaf must still lead
    // exactly as before — a single-column leaf leads with its COLUMN, a multi-column leaf
    // with its NAME — never re-led by the heading-only fix.
    const single = clusterBands([band({ distinguisher: "CDISP" })], id);
    expect(single.showClusterHeadings).toBe(false);
    // Single-column leaf → leads with its column (today's behavior, unchanged).
    expect(single.clusters[0].labeling.bands[0].primary.text).toBe("CDISP");
    expect(single.clusters[0].labeling.bands[0].primaryIsColumn).toBe(true);

    const multi = clusterBands(
      [
        band({
          distinguisher: "agi-multi",
          distinguisherIsColumn: false,
        }),
      ],
      id,
    );
    expect(multi.showClusterHeadings).toBe(false);
    // Lone multi-column leaf → leads with its NAME (no heading to dedup against).
    expect(multi.clusters[0].labeling.bands[0].primary.text).toBe(
      "Disponibel inkomst",
    );
    expect(multi.clusters[0].labeling.bands[0].primary.mono).toBe(false);
  });
});

describe("pickerWindowYears + representationInWindow (#678 dimming)", () => {
  const row = (from: string, to: string) => ({ from, to });

  it("a wire range resolves to its outer year span", () => {
    expect(pickerWindowYears("2010..2015", null)).toEqual([2010, 2015]);
  });

  it("a single token resolves to that token's year span", () => {
    expect(pickerWindowYears("VT2009", null)).toEqual([2009, 2009]);
  });

  it("a comma list unions its parts", () => {
    expect(pickerWindowYears("2005..2008,2012", null)).toEqual([2005, 2012]);
  });

  it("falls back to the study window when no period is active", () => {
    expect(pickerWindowYears(null, { from: 2000, to: 2004 })).toEqual([
      2000, 2004,
    ]);
    // A period that parses to no bound (e.g. _default) also falls back.
    expect(pickerWindowYears("_default", { from: 2000, to: 2004 })).toEqual([
      2000, 2004,
    ]);
  });

  it("is null with neither a parseable period nor a window", () => {
    expect(pickerWindowYears(null, null)).toBeNull();
    expect(pickerWindowYears("_default", null)).toBeNull();
  });

  it("overlap is inclusive; a null window includes everything", () => {
    expect(representationInWindow(row("2010-01-01", "2015-12-31"), null)).toBe(
      true,
    );
    expect(
      representationInWindow(row("2010-01-01", "2015-12-31"), [2012, 2013]),
    ).toBe(true);
    // Touches the boundary year → still overlaps.
    expect(
      representationInWindow(row("2010-01-01", "2012-12-31"), [2012, 2020]),
    ).toBe(true);
    // Entirely before the window → no overlap (dimmed).
    expect(
      representationInWindow(row("2000-01-01", "2005-12-31"), [2012, 2020]),
    ).toBe(false);
  });

  it("an open-ended row reaches past any finite window end", () => {
    expect(
      representationInWindow(row("2010-01-01", "9999-12-31"), [2030, 2040]),
    ).toBe(true);
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

  it("does not pick an arbitrary transition after equal-end overlapping alternatives", () => {
    const states = [
      state({
        state_id: 1,
        value_set_id: 1,
        variant: "individer",
        valid_from: "2020-01-01",
        valid_to: "2020-12-31",
        delivery_column_name: "A",
      }),
      state({
        state_id: 2,
        value_set_id: 1,
        variant: "individer",
        valid_from: "2020-06-01",
        valid_to: "2020-12-31",
        delivery_column_name: "B",
      }),
      state({
        state_id: 3,
        value_set_id: 1,
        variant: "individer",
        valid_from: "2021-01-01",
        valid_to: "2021-12-31",
        delivery_column_name: "C",
      }),
    ];
    const vs = distinctValueSets(states);
    expect(vs[0].usages[0].spans).toEqual([
      { from: "2020-01-01", to: "2021-12-31" },
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

describe("valueSetKeyForColumn (#905 — deep-link column → value set)", () => {
  it("maps a stable-coding column to its distinct value set key", () => {
    const states = [
      state({
        value_set_id: 100,
        delivery_column_name: "COLA",
        valid_from: "2010-01-01",
        valid_to: "2012-12-31",
      }),
      state({
        value_set_id: 200,
        delivery_column_name: "COLB",
        valid_from: "2010-01-01",
        valid_to: "2012-12-31",
      }),
    ];
    expect(valueSetKeyForColumn(states, "COLA")).toBe("id/100");
    expect(valueSetKeyForColumn(states, "COLB")).toBe("id/200");
  });

  it("picks the LATEST-era value set for a coding-varying column", () => {
    // One column delivered two distinct value sets over time — the deep link
    // resolves to the latest-era (max valid_to) coding, matching the picker row's
    // representative.
    const states = [
      state({
        value_set_id: 303,
        delivery_column_name: "COL",
        valid_from: "2015-01-01",
        valid_to: "2018-12-31",
      }),
      state({
        value_set_id: 249,
        delivery_column_name: "COL",
        valid_from: "2019-01-01",
        valid_to: "2022-12-31",
      }),
    ];
    expect(valueSetKeyForColumn(states, "COL")).toBe("id/249");
  });

  it("breaks a valid_to tie by the higher state_id", () => {
    // Two states for the SAME column share an identical latest valid_to — the
    // shared tie-break (max state_id) selects the higher-id state's value set, so
    // the picker row and deep-link resolver stay aligned.
    const states = [
      state({
        state_id: 5,
        value_set_id: 303,
        value_set_version_label: "SNI 2003",
        delivery_column_name: "COL",
        valid_from: "2018-01-01",
        valid_to: "2022-12-31",
      }),
      state({
        state_id: 9,
        value_set_id: 249,
        value_set_version_label: "SNI 2022",
        delivery_column_name: "COL",
        valid_from: "2019-01-01",
        valid_to: "2022-12-31",
      }),
    ];
    const [row] = pickerRepresentations(states);
    expect(row.valueSetLabel).toBe("SNI 2022");
    expect(valueSetKeyForColumn(states, "COL")).toBe("id/249");
  });

  it("resolves a classification column to its slug key (two-level dedup)", () => {
    const states = [
      state({
        value_set_id: 100,
        classification_slug: "lkf2007",
        delivery_column_name: "KOMMUN",
        valid_from: "2007-01-01",
        valid_to: "2010-12-31",
      }),
    ];
    expect(valueSetKeyForColumn(states, "KOMMUN")).toBe("class/lkf2007");
  });

  it("returns null when no state delivers the column", () => {
    const states = [state({ value_set_id: 100, delivery_column_name: "COLA" })];
    expect(valueSetKeyForColumn(states, "NOPE")).toBeNull();
  });

  it("scopes to the given variant when one column is shared across variants (#905)", () => {
    // Two picker rows (keyed `(variant, column)`) share ONE delivery column across
    // different populations with DISTINCT codings. The variant-scoped call must
    // return the CLICKED variant's value set, not the other variant's latest era —
    // the deep-link bug fix. Variant B's coding is more recent overall, so the
    // unscoped (column-only) call would pick B even for an A nudge.
    const states = [
      state({
        variant: "a",
        value_set_id: 100,
        delivery_column_name: "COL",
        valid_from: "2015-01-01",
        valid_to: "2018-12-31",
      }),
      state({
        variant: "b",
        value_set_id: 200,
        delivery_column_name: "COL",
        valid_from: "2019-01-01",
        valid_to: "2022-12-31",
      }),
    ];
    expect(valueSetKeyForColumn(states, "COL", "a")).toBe("id/100");
    expect(valueSetKeyForColumn(states, "COL", "b")).toBe("id/200");
    // Unscoped (back-compat) keeps picking the column's latest era across variants.
    expect(valueSetKeyForColumn(states, "COL")).toBe("id/200");
    // null variant is the same back-compat path (a leaf with no ambiguity).
    expect(valueSetKeyForColumn(states, "COL", null)).toBe("id/200");
  });

  it("variant scoping still resolves a column unique to one variant", () => {
    // A column delivered by only ONE variant: scoping to that variant is a no-op
    // (no behavior change), and scoping to a variant that never delivers it → null.
    const states = [
      state({
        variant: "only",
        value_set_id: 300,
        delivery_column_name: "COL",
      }),
    ];
    expect(valueSetKeyForColumn(states, "COL", "only")).toBe("id/300");
    expect(valueSetKeyForColumn(states, "COL", "absent")).toBeNull();
  });
});

describe("encode/parseCodesParam (#905 — (variant, column) deep-link payload)", () => {
  it("round-trips a (variant, column) pair through the row-key grammar", () => {
    expect(encodeCodesParam("individer", "Yrke")).toBe("individer::Yrke");
    expect(parseCodesParam("individer::Yrke")).toEqual({
      variant: "individer",
      column: "Yrke",
    });
  });

  it("percent-encodes each segment so reserved/non-ASCII chars survive", () => {
    // A variant slug or column with a space / reserved char must not break the URL
    // or the `::` separator parse.
    const encoded = encodeCodesParam("a b", "Kön/2");
    expect(encoded).toBe("a%20b::K%C3%B6n%2F2");
    expect(parseCodesParam(encoded)).toEqual({
      variant: "a b",
      column: "Kön/2",
    });
  });

  it("parses a bare column (no `::`) as variant=null (back-compat / no-variant leaf)", () => {
    expect(parseCodesParam("Yrke")).toEqual({ variant: null, column: "Yrke" });
  });

  it("returns null for an empty / missing param", () => {
    expect(parseCodesParam(null)).toBeNull();
    expect(parseCodesParam(undefined)).toBeNull();
    expect(parseCodesParam("")).toBeNull();
  });

  it("degrades to null (no throw) on a malformed percent-escape (P2: a bad ?codes deep link must not crash the page)", () => {
    // `?codes=` is purely client-side FOCUS state, so a stale/bad deep link must
    // degrade to the default union view, never crash BindingLeafView's render.
    // `decodeURIComponent` THROWS on these — `parseCodesParam` must be total.
    expect(() => parseCodesParam("%")).not.toThrow();
    expect(parseCodesParam("%")).toBeNull();
    // A truncated escape in the COLUMN segment (after a valid variant + `::`).
    expect(() => parseCodesParam("a::%E0%A4%A")).not.toThrow();
    expect(parseCodesParam("a::%E0%A4%A")).toBeNull();
    // A lone malformed bare column (no `::`).
    expect(() => parseCodesParam("%E0%A4%A")).not.toThrow();
    expect(parseCodesParam("%E0%A4%A")).toBeNull();
    // A malformed VARIANT segment also degrades.
    expect(parseCodesParam("%::Yrke")).toBeNull();
  });
});
