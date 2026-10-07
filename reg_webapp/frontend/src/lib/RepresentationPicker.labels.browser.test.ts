import { describe, expect, it, vi } from "vitest";
import { render } from "vitest-browser-svelte";
import RepresentationPicker, {
  type PickerBand,
} from "./RepresentationPicker.svelte";
import {
  atEveryWidth,
  edge,
  graph,
  graphNode,
  graphState,
  lanes,
  one,
  PROPS,
  row,
} from "./representation-picker-test-helpers";

// Split from RepresentationPicker.browser.test.ts by contract surface: cell/row labels: era codings, codings-vary links, lane gutter, rename hints.
// Siblings: RepresentationPicker.{graph,graph-fallback,graph-history,labels,filters,staging,row-identity}.browser.test.ts.

describe("RepresentationPicker graph mode (#904)", () => {
  it("renders each graph cell's own era coding label", async () => {
    const aFqid = "scb/lisa/a";
    const bFqid = "scb/lisa/b";
    await render(RepresentationPicker, {
      bands: [
        {
          key: aFqid,
          name: "A",
          registerPrefix: "scb/lisa",
          rows: [
            row({
              column: "NEW",
              representation: null,
              renamedColumns: ["OLD"],
              valueSetLabel: "New coding",
              from: "1990-01-01",
              to: "2020-12-31",
              period: "1990 – 2020",
            }),
          ],
        } satisfies PickerBand,
        {
          key: bFqid,
          name: "B",
          registerPrefix: "scb/lisa",
          rows: [row({ column: "OTHER" })],
        } satisfies PickerBand,
      ],
      graph: graph({
        nodes: [
          graphNode(aFqid, {
            states: [
              graphState({
                state_id: "1",
                period_scope: "intervals",
                representation_run_id: 1,
                delivery_column_name: "OLD",
                value_set_version_label: "Old coding",
                valid_from: "1990-01-01",
                valid_to: "1999-12-31",
              }),
              graphState({
                state_id: "2",
                period_scope: "intervals",
                representation_run_id: 2,
                delivery_column_name: "NEW",
                value_set_version_label: "New coding",
                valid_from: "2000-01-01",
                valid_to: "2020-12-31",
              }),
            ],
          }),
          graphNode(bFqid, {
            states: [graphState({ delivery_column_name: "OTHER" })],
          }),
        ],
        edges: [edge(bFqid, aFqid)],
        focus_id: null,
      }),
      ...PROPS,
    });

    const oldCell = await vi.waitFor(() => {
      const cell = [
        ...document.querySelectorAll<HTMLElement>(".graph-cell"),
      ].find((el) => el.textContent?.includes("OLD"));
      if (!cell) {
        throw new Error("OLD cell not rendered");
      }
      return cell;
    });
    expect(oldCell.textContent).toContain("Old coding");
    expect(oldCell.textContent).not.toContain("New coding");
  });

  it("links a folded graph cell's codings-vary nudge to that cell's era column", async () => {
    const aFqid = "scb/lisa/a";
    const bFqid = "scb/lisa/b";
    await render(RepresentationPicker, {
      bands: [
        {
          key: aFqid,
          name: "A",
          registerPrefix: "scb/lisa",
          href: "/catalog/scb/lisa/a",
          rows: [
            row({
              variant: "v",
              column: "NEW",
              representation: null,
              renamedColumns: ["OLD"],
              codingsVary: true,
              from: "1990-01-01",
              to: "2020-12-31",
              period: "1990 – 2020",
            }),
          ],
        } satisfies PickerBand,
        {
          key: bFqid,
          name: "B",
          registerPrefix: "scb/lisa",
          rows: [row({ column: "OTHER" })],
        } satisfies PickerBand,
      ],
      graph: graph({
        nodes: [
          graphNode(aFqid, {
            states: [
              graphState({
                state_id: "1",
                period_scope: "intervals",
                representation_run_id: 1,
                delivery_column_name: "OLD",
                value_set_version_label: "Old coding",
                valid_from: "1990-01-01",
                valid_to: "1999-12-31",
              }),
              graphState({
                state_id: "2",
                period_scope: "intervals",
                representation_run_id: 2,
                delivery_column_name: "NEW",
                value_set_version_label: "New coding",
                valid_from: "2000-01-01",
                valid_to: "2020-12-31",
              }),
            ],
          }),
          graphNode(bFqid, {
            states: [graphState({ delivery_column_name: "OTHER" })],
          }),
        ],
        edges: [edge(bFqid, aFqid)],
        focus_id: null,
      }),
      ...PROPS,
    });

    const oldCell = await vi.waitFor(() => {
      const cell = [
        ...document.querySelectorAll<HTMLElement>(".graph-cell"),
      ].find((el) => el.textContent?.includes("OLD"));
      if (!cell) {
        throw new Error("OLD cell not rendered");
      }
      return cell;
    });
    const nudge = oldCell.querySelector<HTMLAnchorElement>(".codings-vary");
    expect(nudge?.getAttribute("href")).toBe(
      "/catalog/scb/lisa/a?codes=v%3A%3AOLD#states-heading",
    );
  });

  it("attributes a folded FAMILY cell's codings-vary link to its concrete era variant", async () => {
    // #376 regression: a folded variant-family row folds TWO concrete variants
    // (individer-16plus predecessor era + individer-15plus successor era) delivering
    // one column `Kon`. Each era renders its own graph cell (carrying its concrete
    // `cell.variant`). The codings-vary deep link must target the cell's CONCRETE
    // variant, NOT the head `row.variant` — else the predecessor cell would link to a
    // `(individer-15plus, Kon)` coding the successor never delivered in that era.
    const fqid = "scb/lisa/kon";
    await render(RepresentationPicker, {
      bands: [
        {
          key: fqid,
          name: "Kön",
          registerPrefix: "scb/lisa",
          href: "/catalog/scb/lisa/kon",
          rows: [
            row({
              key: "individer-15plus{individer-16plus,individer-15plus}::Kon",
              variant: "individer-15plus",
              variantLabel: "Individer, 15 år och äldre",
              variantFamily: "individer-15plus",
              variantFamilyLabel: "Individer",
              column: "Kon",
              codingsVary: true,
              from: "1990-01-01",
              to: "2023-12-31",
              windows: [
                { from: "1990-01-01", to: "2009-12-31" },
                { from: "2010-01-01", to: "2023-12-31" },
              ],
              variantSegments: [
                {
                  variant: "individer-16plus",
                  variantLabel: "Individer, 16 år och äldre",
                  windows: [{ from: "1990-01-01", to: "2009-12-31" }],
                },
                {
                  variant: "individer-15plus",
                  variantLabel: "Individer, 15 år och äldre",
                  windows: [{ from: "2010-01-01", to: "2023-12-31" }],
                },
              ],
              period: "1990 – 2023",
              wirePeriod: "1990..2009,2010..2023",
            }),
          ],
        } satisfies PickerBand,
      ],
      graph: graph({
        nodes: [
          graphNode(fqid, {
            states: [
              graphState({
                state_id: "1",
                period_scope: "intervals",
                variant: "individer-16plus",
                representation_run_id: 1,
                delivery_column_name: "Kon",
                value_set_id: "10",
                value_set_version_label: "16+ coding",
                valid_from: "1990-01-01",
                valid_to: "2009-12-31",
              }),
              graphState({
                state_id: "2",
                period_scope: "intervals",
                variant: "individer-15plus",
                representation_run_id: 2,
                delivery_column_name: "Kon",
                value_set_id: "20",
                value_set_version_label: "15+ coding",
                valid_from: "2010-01-01",
                valid_to: "2023-12-31",
              }),
            ],
          }),
        ],
        edges: [],
        focus_id: fqid,
      }),
      ...PROPS,
    });

    const hrefs = await vi.waitFor(() => {
      const found = [
        ...document.querySelectorAll<HTMLLabelElement>("label.graph-cell"),
      ]
        .map((c) =>
          c
            .querySelector<HTMLAnchorElement>(".codings-vary")
            ?.getAttribute("href"),
        )
        .filter((h): h is string => h != null);
      if (found.length < 2) {
        throw new Error("both era coding links not yet rendered");
      }
      return found;
    });
    // Each era's link carries ITS OWN concrete variant, never both collapsed to the head.
    expect(hrefs).toContain(
      "/catalog/scb/lisa/kon?codes=individer-16plus%3A%3AKon#states-heading",
    );
    expect(hrefs).toContain(
      "/catalog/scb/lisa/kon?codes=individer-15plus%3A%3AKon#states-heading",
    );
  });

  // Y-14: the reproduced lane — the VIEWED leaf (`/catalog/scb/lisa/kon?period=2018`
  // after adding Kon) carries four gutter lines (name, slug, "Viewed", "also in
  // rams") in a lane sized only for its cells, so `.graph-timeline`'s clip cut the
  // last line off and the lane above ran into its neighbour. The lane now budgets
  // its measured gutter, at every width the design language covers.
  it("keeps a viewed lane's name, slug, Viewed marker and same-as links inside the lane at every width", async () => {
    const konFqid = "scb/lisa/kon";
    const sysFqid = "scb/rams/syss";
    await render(RepresentationPicker, {
      bands: [
        {
          key: konFqid,
          name: "Kön",
          registerPrefix: "scb/lisa",
          rows: [row({ column: "Kon" })],
        } satisfies PickerBand,
      ],
      graph: graph({
        nodes: [
          graphNode(konFqid, {
            // The leaf shape: no concept group, so the lane leads with the
            // variable's NAME over its slug (the wrapping case), not the slug alone.
            label: "Kön",
            group_key: null,
            group_label: null,
            states: [graphState({ delivery_column_name: "Kon" })],
            same_as: [{ fqid: "scb/rams/kon", register: "rams" }],
          }),
          graphNode(sysFqid, {
            label: "Sysselsättningsstatus för individ",
            group_key: null,
            group_label: null,
            states: [
              graphState({
                state_id: "2",
                period_scope: "intervals",
                representation_run_id: 2,
                delivery_column_name: "Syss",
                valid_from: "2011-01-01",
                valid_to: "9999-12-31",
              }),
            ],
          }),
        ],
        edges: [edge(konFqid, sysFqid)],
        focus_id: konFqid,
      }),
      ...PROPS,
      focusKey: konFqid,
    });

    await vi.waitFor(() => {
      if (!document.querySelector(".graph-same-as")) {
        throw new Error("graph picker not rendered");
      }
    });

    await atEveryWidth(async () => {
      // The lane budgets a MEASURED gutter, so let the resize settle before the
      // geometry is read.
      await vi.waitFor(() => {
        expect(lanes()).toHaveLength(2);
        const timeline =
          one<HTMLElement>(".graph-timeline").getBoundingClientRect();
        for (const lane of lanes()) {
          const laneBox = lane.getBoundingClientRect();
          // Every gutter line the lane renders — including the "also in" links
          // of the viewed lane — sits inside the lane, so nothing is clipped by
          // the timeline and nothing is drawn over the next lane.
          const lines = [
            ...lane.querySelectorAll<HTMLElement>(
              ".graph-name, .graph-slug, .graph-viewed, .graph-same-as",
            ),
          ];
          expect(lines.length).toBeGreaterThan(0);
          for (const line of lines) {
            const box = line.getBoundingClientRect();
            expect(box.height).toBeGreaterThan(0);
            expect(box.top).toBeGreaterThanOrEqual(laneBox.top);
            expect(box.bottom).toBeLessThanOrEqual(laneBox.bottom);
            expect(box.bottom).toBeLessThanOrEqual(timeline.bottom);
          }
        }
      });

      // A lane grown for its gutter moves the next one down rather than
      // overlapping it.
      const [viewed, successor] = lanes();
      expect(successor.getBoundingClientRect().top).toBeGreaterThanOrEqual(
        viewed.getBoundingClientRect().bottom,
      );
    });
  });
});

describe("codingsVaryNudge deep link (#905)", () => {
  /** Render one band whose single coding-varying row carries the nudge, then return
   * the nudge anchor's `href`. */
  async function nudgeHref(band: PickerBand): Promise<string> {
    await render(RepresentationPicker, { bands: [band], axes: [], ...PROPS });
    const nudge = await vi.waitFor(() => {
      const a = document.querySelector(".rep-picker .codings-vary");
      if (!a) {
        throw new Error("codings-vary nudge not yet rendered");
      }
      return a;
    });
    expect(nudge.tagName).toBe("A");
    return nudge.getAttribute("href") ?? "";
  }

  // The nudge means "this column's coding changed OVER TIME — see the value sets",
  // which is inherently a FULL-HISTORY inspection (#905, Codex P2). So when `band.href`
  // carries an active `?period`, the nudge DROPS it (taking only the member path) and
  // emits a CLEAN `?codes=…` query — a period-narrowed leaf, focused via `?codes`, must
  // show the column's full coding history, not the period-scoped subset.
  it("drops an inherited ?period and emits a clean ?codes-only deep link", async () => {
    const href = await nudgeHref({
      key: "scb/lisa/yrkesreg",
      name: "Yrkesregistret",
      registerPrefix: "scb/lisa",
      href: "/catalog/scb/lisa/yrkesreg?period=2020",
      rows: [row({ variant: "v", column: "Yrke", codingsVary: true })],
    } satisfies PickerBand);
    // No `period` survives; the only param is the (variant, column) composite.
    expect(href).toBe(
      "/catalog/scb/lisa/yrkesreg?codes=v%3A%3AYrke#states-heading",
    );
    expect(href).not.toContain("period");
    // Exactly ONE `?` (no double query separator).
    expect(href.match(/\?/g)).toHaveLength(1);
  });
});

describe("RepresentationPicker sequential-rename hint (#902)", () => {
  // The picker collapses a variable's sequential column RENAME (non-overlapping eras,
  // distinct names) into ONE row led by the latest column, surfacing the earlier name(s)
  // as a quiet inline ".rename-hint" ("was OldCol"). This is normally produced by
  // pickerRepresentations; here we feed the collapsed `renamedColumns` directly to test
  // the render path (the `{@render renameHint(...)}` snippet) in isolation.
  it("renders a 'was <old>' hint for a collapsed rename, and none when empty", async () => {
    await render(RepresentationPicker, {
      bands: [
        {
          key: "scb/x/renamed",
          name: "Renamed",
          registerPrefix: "scb/x",
          rows: [row({ column: "NewCol", renamedColumns: ["OldCol"] })],
        } satisfies PickerBand,
        {
          key: "scb/x/plain",
          name: "Plain",
          registerPrefix: "scb/x",
          rows: [row({ column: "PlainCol", renamedColumns: [] })],
        } satisfies PickerBand,
      ],
      axes: [],
      ...PROPS,
    });
    await vi.waitFor(() => {
      if (document.querySelectorAll(".col-row .col-chip").length < 2) {
        throw new Error("rows not rendered yet");
      }
    });

    // The renamed band shows exactly one ".rename-hint" naming the earlier column.
    const hints = [...document.querySelectorAll(".rename-hint")];
    expect(hints).toHaveLength(1);
    expect(hints[0].textContent?.trim()).toBe("was OldCol");

    // The plain band's row carries NO ".rename-hint".
    const plainRow = [...document.querySelectorAll(".col-row")].find((r) =>
      r.textContent?.includes("PlainCol"),
    );
    expect(plainRow?.querySelector(".rename-hint")).toBeNull();
  });
});
