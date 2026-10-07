import { describe, expect, it } from "vitest";
import type { GraphState, VariableStateModel } from "./api";
import type {
  PickerBandFacets,
  PickerDimension,
  PickerRepresentation,
} from "./catalog";
import {
  addWindowBounds,
  catalogHref,
  coexistingColumns,
  coverageFromStates,
  DATA_BROWSER_LABEL,
  deliveryColumnNamesFromStates,
  deriveType,
  distinctValueSets,
  encodeCodesParam,
  leafSlug,
  narrowStatesByModifier,
  parseCodesParam,
  pickerFilterDimensions,
  pickerRepresentations,
  pickerRowPasses,
  rankFilter,
  representationInWindow,
  routeBreadcrumbs,
  rowAddPeriod,
  valueSetKeyForColumn,
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
    warning_ids: [],
    state_id: "1",
    period_scope: "intervals",
    variant: "v",
    variant_label: null,
    register_variant_id: "1",
    valid_from: "",
    valid_to: "",
    data_type: null,
    data_length: null,
    delivery_column_name: null,
    source_register_text: null,
    provenance: null,
    pooled: false,
    value_set_version_label: "",
    value_set_id: null,
    value_set: null,
    value_set_summary: null,
    is_identifier: false,
    classifications: [],

    ...over,
  };
}

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

describe("routeBreadcrumbs", () => {
  // The topbar trail is STRUCTURAL (raw segments + the data-browser root); the
  // routed page owns its rich header. The LAST crumb is always the current page
  // (no href — Breadcrumbs renders it as aria-current="page").
  const browserRoot = { label: DATA_BROWSER_LABEL, href: catalogHref("") };

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

describe("deriveType", () => {
  // The denseness verdict is the SERVER's now (reg_meta `dense_integer_range`,
  // tested there over the members) — the leaf carries it, so here we only check
  // that a state carrying one is read as a measure rather than a category.
  it("dense integer age value sets stay numeric, not categorical", () => {
    expect(
      deriveType(
        state({
          value_set_id: "5",
          value_set_summary: {
            code_count: 111,
            integer_range: { min: 0, max: 110 },
          },
          data_type: "char",
        }),
      ),
    ).toBe("numeric");
  });

  it("keeps small numeric and labelled codebooks categorical", () => {
    expect(
      deriveType(
        state({
          value_set_id: "5",
          value_set_summary: { code_count: 2, integer_range: null },
          data_type: "int",
        }),
      ),
    ).toBe("categorical");
    expect(
      deriveType(
        state({
          value_set_id: "6",
          value_set_summary: { code_count: 10, integer_range: null },
          data_type: "int",
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
        state({ data_type: "int", is_identifier: true, value_set_id: "5" }),
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

// ── Concept-group folding (#303) ─────────────────────────────────────────────

import type { ConceptGroup } from "./api";
import {
  foldGroupedRows,
  groupFilterKeys,
  narrowGroupsToMembers,
} from "./catalog";

function group(over: Partial<ConceptGroup>): ConceptGroup {
  return {
    key: "ink",
    label: "Inkomst",
    source: "token",
    axes: ax("month"),
    tags: [],
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

describe("narrowGroupsToMembers (Y-82 variant lens)", () => {
  const threeMember = group({
    members: [
      ...group({}).members,
      {
        fqid: "scb/lisa/inkmar",
        name: "Inkomst i mars",
        facets: [{ axis: "month", value: "03", label: "mars" }],
      },
    ],
  });

  /** A lensed `BindingChild`-shaped item: an fqid delivered under the given
   * columns (null, the default, for a whole-variable delivery — the
   * SCB-named-no-column case `delivery_column` is also null for). */
  function delivered(
    fqid: string,
    ...columns: (string | null)[]
  ): { fqid: string; deliveries: { column: string | null }[] } {
    return {
      fqid,
      deliveries: (columns.length > 0 ? columns : [null]).map((column) => ({
        column,
      })),
    };
  }

  it("drops a group the lens leaves with one member — that member is a leaf row", () => {
    expect(
      narrowGroupsToMembers([threeMember], [delivered("scb/lisa/inkjan")]),
    ).toEqual([]);
  });

  it("counts DISTINCT members, so a representation group of one variable goes too", () => {
    // #819: representation members share one `fqid` across delivery columns —
    // two members on one variable are still one variable, not a group.
    const rep = group({
      members: [
        {
          fqid: "scb/lisa/disp",
          name: "Disp",
          facets: [],
          delivery_column: "CDISP",
        },
        {
          fqid: "scb/lisa/disp",
          name: "Disp",
          facets: [],
          delivery_column: "CDISP5",
        },
      ],
    });
    expect(
      narrowGroupsToMembers(
        [rep],
        [delivered("scb/lisa/disp", "CDISP", "CDISP5")],
      ),
    ).toEqual([]);
  });
});

describe("groupFilterKeys", () => {
  /** The register page supplies real delivery columns (Y-82); these cases are
   * about the group's OWN keys, so they hand it nothing. */
  const noColumns = (): string[] => [];

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
    const keys = groupFilterKeys(repGroup, noColumns);
    expect(keys).toContain("CDISP5");
    expect(keys).toContain("Exkl. kapitalvinst");
    // And rankFilter actually surfaces the group on those needles.
    const rows = [{ id: "other" }, { id: "disp-group", group: repGroup }];
    const keysOf = (r: (typeof rows)[number]): (string | null | undefined)[] =>
      "group" in r && r.group
        ? groupFilterKeys(r.group, noColumns)
        : ["unrelated"];
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
      r.group ? groupFilterKeys(r.group, noColumns) : [`zzz-inkjan-zzz`];
    const ranked = rankFilter(rows, "inkjan", keysOf);
    expect(ranked[0].id).toBe("ink-group");
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
        state_id: "1",
        period_scope: "intervals",
        representation_run_id: 1,
        variant: "individer",
        variant_label: null,
        delivery_column_name: null,
        value_set_version_label: "1-siffrig",
        value_set_id: null,
        valid_from: null,
        valid_to: null,
        classification_slugs: [],
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
        state_id: "2",
        period_scope: "intervals",
        representation_run_id: 1,
        variant: "v1",
        variant_label: null,
        delivery_column_name: "Col",
        value_set_version_label: "",
        value_set_id: null,
        valid_from: null, // unknown start
        valid_to: "2008-12-31",
        classification_slugs: [],
      } as GraphState,
    ]);
    expect(row.from).toBe("0001-01-01");
    expect(row.to).toBe("2008-12-31");
    // The one-sided "until <year>" form, never the leaked sentinel year.
    expect(row.period).toBe("until 2008");
  });

  it("does NOT flag codingsVary when one value_set_id has inconsistent LABELS (the SUN case)", () => {
    // The same id 249 is labelled inconsistently across years/populations ('old' /
    // 'SUN 2020 NivaOld'). Keyed on the reliable id, this is ONE coding → no nudge.
    const [row] = pickerRepresentations([
      state({
        variant: "v1",
        delivery_column_name: "Sun",
        value_set_id: "249",
        value_set_version_label: "SUN 2020 NivaOld",
        valid_from: "2020-01-01",
        valid_to: "2020-12-31",
      }),
      state({
        variant: "v1",
        delivery_column_name: "Sun",
        value_set_id: "249",
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
        value_set_id: "42",
        valid_from: "2019-01-01",
        valid_to: "2019-12-31",
      }),
    ]);
    expect(row.codingsVary).toBe(true);
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

describe("deliveryColumnNamesFromStates (the cart's unpinned column name)", () => {
  it("leads a rename within the period with the CURRENT column, superseded ones after", () => {
    // DINF → DINF83 → DINF86 over non-overlapping eras is ONE column renamed, the
    // same fold the picker's own rows present.
    expect(
      deliveryColumnNamesFromStates([
        state({
          delivery_column_name: "DINF83",
          valid_from: "1984-01-01",
          valid_to: "1985-12-31",
        }),
        state({
          delivery_column_name: "DINF",
          valid_from: "1981-01-01",
          valid_to: "1983-12-31",
        }),
        state({
          delivery_column_name: "DINF86",
          valid_from: "1990-01-01",
          valid_to: "9999-12-31",
        }),
      ]),
    ).toEqual(["DINF86", "DINF83", "DINF"]);
  });

  it("names nothing when co-existing columns leave a choice the file must pin", () => {
    // Two columns valid at the same instant: the cart may not pick one of them for
    // the researcher, so the row falls back to its FQID.
    expect(
      deliveryColumnNamesFromStates([
        state({
          delivery_column_name: "Ssyk3",
          valid_from: "2010-01-01",
          valid_to: "2020-12-31",
        }),
        state({
          delivery_column_name: "Ssyk4",
          valid_from: "2012-01-01",
          valid_to: "2020-12-31",
        }),
      ]),
    ).toEqual([]);
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
    // A window inside the GAP keeps neither era → nothing to commit.
    expect(rowAddPeriod(disjoint, yr(2012, 2013))).toBeNull();
    // A window over only the first era keeps just it.
    expect(rowAddPeriod(disjoint, yr(2006, 2009))).toBe("2006..2009");
  });
});

describe("addWindowBounds (#678 finding 1: sub-annual period honored on add)", () => {
  it("a ?period that parses to no bound (_default) falls back to the year window", () => {
    expect(addWindowBounds("_default", [2000, 2004])).toEqual({
      from: "2000-01-01",
      to: "2004-12-31",
    });
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
        label: "Variant",
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

describe("pickerWindowYears + representationInWindow (#678 dimming)", () => {
  const row = (from: string, to: string) => ({ from, to });

  it("an open-ended row reaches past any finite window end", () => {
    expect(
      representationInWindow(row("2010-01-01", "9999-12-31"), [2030, 2040]),
    ).toBe(true);
  });
});

describe("coverageFromStates (#615 availability span)", () => {
  it("the open-ended sentinel leaves the END unbounded (null), start preserved", () => {
    // `9999-12-31` = "still delivered" → `to: null`; the picker projects the open
    // end to the slider's vintage ceiling, never a literal 9999 track.
    expect(
      coverageFromStates([
        state({
          state_id: "1",
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
          state_id: "1",
          valid_from: "0001-01-01",
          valid_to: "2008-12-31",
        }),
      ]),
    ).toEqual({ from: null, to: 2008 });
  });

  it("a wholly-sentinel state (0001..9999) is unbounded on BOTH sides → null", () => {
    // Both bounds are sentinels, so coverage is fully unknown — no finite side to
    // draw or gap against (NOT { from: 1, … }, which would let the slider emit
    // out-of-grammar wires like `1..2026`).
    expect(
      coverageFromStates([
        state({
          state_id: "1",
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
          state_id: "1",
          valid_from: "0001-01-01",
          valid_to: "2008-12-31",
        }),
        state({
          state_id: "2",
          valid_from: "2002-01-01",
          valid_to: "2010-12-31",
        }),
      ]),
    ).toEqual({ from: 2002, to: 2010 });
  });
});

describe("distinctValueSets (#668 — value-set-centric fold)", () => {
  it("lists which variants use a value set (the cross-variant case)", () => {
    const states = [
      state({ value_set_id: "1", variant: "doda", valid_from: "1983-01-01" }),
      state({ value_set_id: "1", variant: "fodda", valid_from: "1983-01-01" }),
    ];
    const vs = distinctValueSets(states);
    expect(vs[0].usages.map((u) => u.variant).sort()).toEqual([
      "doda",
      "fodda",
    ]);
  });

  it.each([
    ["continuing column first", 2, 3, false],
    ["added alias first", 3, 2, true],
  ])(
    "does not report a replacement when a successor adds an alias (%s)",
    (_label, continuingStateId, aliasStateId, reverseSuccessors) => {
      const predecessor = state({
        state_id: "1",
        value_set_id: "1",
        variant: "individer",
        valid_from: "2017-01-01",
        valid_to: "2017-12-31",
        delivery_column_name: "A",
      });
      const continuing = state({
        state_id: String(continuingStateId),
        value_set_id: "1",
        variant: "individer",
        valid_from: "2018-01-01",
        valid_to: "2018-12-31",
        delivery_column_name: "A",
      });
      const alias = state({
        state_id: String(aliasStateId),
        value_set_id: "1",
        variant: "individer",
        valid_from: "2018-01-01",
        valid_to: "2018-12-31",
        delivery_column_name: "B",
      });
      const successors = reverseSuccessors
        ? [alias, continuing]
        : [continuing, alias];

      const usage = distinctValueSets([predecessor, ...successors])[0]
        .usages[0];
      expect(
        usage.states
          .filter((s) => s.valid_from === "2018-01-01")
          .map((s) => s.delivery_column_name),
      ).toEqual(["A", "B"]);
      expect(usage.spans).toEqual([
        { from: "2017-01-01", to: "2018-12-31", pooled: false },
      ]);
    },
  );

  it("does not report technical changes for same-state monthly windows", () => {
    const states = [
      state({
        state_id: "10",
        value_set_id: "1",
        variant: "individer",
        valid_from: "2020-01-01",
        valid_to: "2020-01-31",
        delivery_column_name: "LonFinkJan",
      }),
      state({
        state_id: "10",
        value_set_id: "1",
        variant: "individer",
        valid_from: "2020-02-01",
        valid_to: "2020-02-29",
        delivery_column_name: "LonFinkFeb",
      }),
    ];
    const vs = distinctValueSets(states);
    expect(vs[0].usages[0].spans).toEqual([
      { from: "2020-01-01", to: "2020-02-29", pooled: false },
    ]);
  });

  it("does not report technical changes for overlapping alternatives", () => {
    const states = [
      state({
        value_set_id: "1",
        variant: "individer",
        valid_from: "2020-01-01",
        valid_to: "2020-12-31",
        delivery_column_name: "A",
      }),
      state({
        value_set_id: "1",
        variant: "individer",
        valid_from: "2020-06-01",
        valid_to: "2021-12-31",
        delivery_column_name: "B",
      }),
    ];
    const vs = distinctValueSets(states);
    expect(vs[0].usages[0].spans).toEqual([
      { from: "2020-01-01", to: "2021-12-31", pooled: false },
    ]);
  });

  it("keeps the span-end predecessor after a contained overlap", () => {
    const states = [
      state({
        state_id: "1",
        value_set_id: "1",
        variant: "individer",
        valid_from: "2020-01-01",
        valid_to: "2021-12-31",
        data_type: "int",
        delivery_column_name: "A",
      }),
      state({
        state_id: "2",
        value_set_id: "1",
        variant: "individer",
        valid_from: "2021-01-01",
        valid_to: "2021-06-30",
        data_type: "char",
        delivery_column_name: "B",
      }),
      state({
        state_id: "3",
        value_set_id: "1",
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
        pooled: false,
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
        state_id: "1",
        value_set_id: "1",
        variant: "individer",
        valid_from: "2020-01-01",
        valid_to: "2020-12-31",
        delivery_column_name: "A",
      }),
      state({
        state_id: "2",
        value_set_id: "1",
        variant: "individer",
        valid_from: "2020-06-01",
        valid_to: "2020-12-31",
        delivery_column_name: "B",
      }),
      state({
        state_id: "3",
        value_set_id: "1",
        variant: "individer",
        valid_from: "2021-01-01",
        valid_to: "2021-12-31",
        delivery_column_name: "C",
      }),
    ];
    const vs = distinctValueSets(states);
    expect(vs[0].usages[0].spans).toEqual([
      { from: "2020-01-01", to: "2021-12-31", pooled: false },
    ]);
  });

  it("does not merge across different variants (spans are per-variant)", () => {
    const states = [
      state({
        value_set_id: "1",
        variant: "doda",
        valid_from: "2000-01-01",
        valid_to: "2000-12-31",
      }),
      state({
        value_set_id: "1",
        variant: "fodda",
        valid_from: "2001-01-01",
        valid_to: "2001-12-31",
      }),
    ];
    const vs = distinctValueSets(states);
    const doda = vs[0].usages.find((u) => u.variant === "doda");
    const fodda = vs[0].usages.find((u) => u.variant === "fodda");
    expect(doda?.spans).toEqual([
      { from: "2000-01-01", to: "2000-12-31", pooled: false },
    ]);
    expect(fodda?.spans).toEqual([
      { from: "2001-01-01", to: "2001-12-31", pooled: false },
    ]);
  });

  it("collapses contiguous years across the UNION of ids in one classification edition (M20)", () => {
    // Two distinct value_set_ids share `lkf1980` (the M13 collapse) and deliver
    // adjacent years (1980, 1981) under the SAME variant. The per-variant M20
    // collapse runs over the UNION of those ids' states, so they fuse into ONE
    // span — not one per id (which would leave two adjacent rows).
    const states = [
      state({
        value_set_id: "100",
        classifications: [
          {
            slug: "lkf1980",
            short_name: "lkf1980",
            name: "lkf1980",
            conformance: null,
          },
        ],
        variant: "doda",
        valid_from: "1980-01-01",
        valid_to: "1980-12-31",
      }),
      state({
        value_set_id: "101", // distinct id, SAME edition + variant + adjacent year
        classifications: [
          {
            slug: "lkf1980",
            short_name: "lkf1980",
            name: "lkf1980",
            conformance: null,
          },
        ],
        variant: "doda",
        valid_from: "1981-01-01",
        valid_to: "1981-12-31",
      }),
    ];
    const vs = distinctValueSets(states);
    expect(vs).toHaveLength(2);
    expect(vs[0].usages).toHaveLength(1);
    expect(vs[0].usages[0].spans).toEqual([
      { from: "1980-01-01", to: "1980-12-31", pooled: false },
    ]);
  });

  it("collapseSpans: overlapping windows extend into one span", () => {
    // Two states whose windows OVERLAP (not merely back-to-back) fuse into a
    // single span spanning the outer bounds.
    const states = [
      state({
        value_set_id: "1",
        variant: "doda",
        valid_from: "2000-01-01",
        valid_to: "2003-12-31",
      }),
      state({
        value_set_id: "1",
        variant: "doda",
        valid_from: "2002-01-01", // starts INSIDE the first window
        valid_to: "2005-12-31",
      }),
    ];
    const vs = distinctValueSets(states);
    expect(vs[0].usages[0].spans).toEqual([
      { from: "2000-01-01", to: "2005-12-31", pooled: false },
    ]);
  });

  it("collapseSpans: a real >1-day gap splits into two spans", () => {
    // A multi-day gap between windows (not a same-day continuation) starts a new
    // span — the day-after adjacency test must NOT fuse across it.
    const states = [
      state({
        value_set_id: "1",
        variant: "doda",
        valid_from: "2000-01-01",
        valid_to: "2000-06-30",
      }),
      state({
        value_set_id: "1",
        variant: "doda",
        valid_from: "2000-08-01", // a one-month gap after 2000-06-30
        valid_to: "2000-12-31",
      }),
    ];
    const vs = distinctValueSets(states);
    expect(vs[0].usages[0].spans).toEqual([
      { from: "2000-01-01", to: "2000-06-30", pooled: false },
      { from: "2000-08-01", to: "2000-12-31", pooled: false },
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
        value_set_id: "1",
        variant: "doda",
        valid_from: "2016-01-01",
        valid_to: "9999-12-31",
      }),
      state({
        value_set_id: "1",
        variant: "doda",
        valid_from: "2020-01-01",
        valid_to: "9999-12-31",
      }),
    ];
    const vs = distinctValueSets(states);
    expect(vs[0].usages[0].spans).toEqual([
      { from: "2016-01-01", to: "9999-12-31", pooled: false },
    ]);
  });
});

describe("valueSetKeyForColumn (#905 — deep-link column → value set)", () => {
  it("breaks a valid_to tie by the higher state_id", () => {
    // Two states for the SAME column share an identical latest valid_to — the
    // shared tie-break (max state_id) selects the higher-id state's value set, so
    // the picker row and deep-link resolver stay aligned.
    const states = [
      state({
        state_id: "5",
        value_set_id: "303",
        value_set_version_label: "SNI 2003",
        delivery_column_name: "COL",
        valid_from: "2018-01-01",
        valid_to: "2022-12-31",
      }),
      state({
        state_id: "9",
        value_set_id: "249",
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

describe("year-independent delivery", () => {
  it("does not infer calendar co-delivery between independent and dated states", () => {
    expect(
      coexistingColumns([
        state({
          period_scope: "year_independent",
          valid_from: null,
          valid_to: null,
          delivery_column_name: "Independent",
        }),
        state({
          valid_from: "2020-01-01",
          valid_to: "2020-12-31",
          delivery_column_name: "Dated",
        }),
      ]),
    ).toEqual(new Set());
  });
});
