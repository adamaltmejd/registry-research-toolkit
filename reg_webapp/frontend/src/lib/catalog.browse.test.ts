import { describe, expect, it } from "vitest";
import {
  catalogHref,
  DATA_BROWSER_LABEL,
  deriveType,
  leafSlug,
  rankFilter,
  routeBreadcrumbs,
} from "./catalog";
import { ax, state } from "./catalog-test-helpers";
import type { Route } from "./router.svelte";

// Split from catalog.test.ts by contract surface: browse helpers.
// Siblings: catalog.{browse,picker,picker-filters,value-sets}.test.ts.

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
    fqid: "group/scb/lisa/ink",
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
