import { describe, expect, it, vi } from "vitest";
import { page } from "vitest/browser";
import { render } from "vitest-browser-svelte";
import type { GraphEdge, RelationshipGraph } from "./api";
import { OPEN_ENDED_VALID_TO, pickerRepresentations } from "./catalog";
import { CELL_MIN_W } from "./picker_graph";
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

// Split from RepresentationPicker.browser.test.ts by contract surface: coexisting-variant row identity (Y-14).
// Siblings: RepresentationPicker.{graph,graph-fallback,graph-history,labels,filters,staging,row-identity}.browser.test.ts.

/** Do two painted boxes share more than `slack` px of area in BOTH axes — i.e. does
 * one paint over the other? */
function overlaps(a: DOMRect, b: DOMRect, slack = 0.5): boolean {
  return (
    a.left < b.right - slack &&
    b.left < a.right - slack &&
    a.top < b.bottom - slack &&
    b.top < a.bottom - slack
  );
}

/** The boxes the element's own TEXT actually paints in — one per line box, read off a
 * range over the text rather than off the element, whose box is as tall as however many
 * lines the text broke into and says nothing about where the glyphs landed. */
function glyphLines(el: Element): DOMRect[] {
  const range = document.createRange();
  range.selectNodeContents(el);
  return [...range.getClientRects()];
}

/** The visible text of everything matching `sel`, whitespace-collapsed. */
function texts(sel: string, within: ParentNode = document): string[] {
  return [...within.querySelectorAll(sel)].map((el) =>
    (el.textContent ?? "").replace(/\s+/g, " ").trim(),
  );
}

/** Wait until the picker has rendered its graph/time-band mode. */
async function graphPickerRendered(): Promise<void> {
  await vi.waitFor(() => {
    if (!document.querySelector(".graph-picker")) {
      throw new Error("graph picker not rendered");
    }
  });
}

// Y-14: a researcher choosing a delivery representation must be able to tell two
// register variants apart. `Kon` over 2018 in coexisting populations is the case:
// with distinct curator names the names do it (the shipped behavior), and when a
// curator gave two of them the SAME name the concrete family slug does.
describe("RepresentationPicker coexisting-variant row identity (Y-14)", () => {
  const PERIOD = { valid_from: "2018-01-01", valid_to: "2018-12-31" } as const;
  // The two curator namings the ticket turns on: names that already differ, and one
  // name on both populations.
  const DISTINCT: [string, string] = ["Individer 15+", "Individer 16+"];
  const ALIKE: [string, string] = ["Individer", "Individer"];
  // …and a third naming where one curator name IS another population's slug.
  const NAME_IS_SLUG = ["Individer", "Individer", "individer-15plus"] as const;
  // The concrete populations the curator names, in fixture order.
  const VARIANTS = [
    "individer-15plus",
    "individer-16plus",
    "individer-17plus",
  ] as const;

  /** ONE delivery column over the same period in AS MANY populations as `labels`
   * names, named by the curator as given — distinct names, one name twice, or a name
   * that is another population's slug — as a band AND the matching one-node graph:
   * coexisting runs on one lane, the shape the ticket's own
   * `/catalog/scb/lisa/kon?period=2018` leaf has, so the picker prefers its time-band
   * mode. One spec builds both, so the rows and the cells cannot drift apart. */
  function variantFixture(
    labels: readonly string[],
    over: {
      period?: { valid_from: string; valid_to: string };
      coding?: string;
      column?: string;
    } = {},
  ): { bands: PickerBand[]; graph: RelationshipGraph } {
    const period = over.period ?? PERIOD;
    const coding = over.coding ?? "";
    const column = over.column ?? "Kon";
    const populations = labels.map((label, i) => ({
      variant: VARIANTS[i],
      label,
    }));
    return {
      bands: [
        {
          key: "scb/lisa/kon",
          name: "Kön",
          registerPrefix: "scb/lisa",
          rows: pickerRepresentations(
            populations.map((p, i) => ({
              state_id: String(i + 1),
              period_scope: "intervals",
              variant: p.variant,
              variant_label: p.label,
              delivery_column_name: column,
              value_set_version_label: coding,
              value_set_id: null,
              ...period,
            })),
          ),
        } satisfies PickerBand,
      ],
      graph: graph({
        nodes: [
          graphNode("scb/lisa/kon", {
            label: "Kön",
            states: populations.map((p, i) =>
              graphState({
                state_id: String(i + 1),
                period_scope: "intervals",
                representation_run_id: i + 1,
                variant: p.variant,
                variant_label: p.label,
                delivery_column_name: column,
                value_set_version_label: coding,
                ...period,
              }),
            ),
          }),
        ],
        focus_id: "scb/lisa/kon",
      }),
    };
  }

  const ROWS = ".col-row.nested .row-btn";

  it("distinct curator names identify the rows on their own — no slug added", async () => {
    await render(RepresentationPicker, {
      bands: variantFixture(["Individer 15+", "Individer 16+"]).bands,
      ...PROPS,
    });

    expect(texts(ROWS)).toEqual(["Individer 15+ 2018", "Individer 16+ 2018"]);
    // The adaptive labeling is untouched: the shared column stays hoisted context
    // and no row repeats an identifier it doesn't need.
    expect(document.querySelector(".variant-key")).toBeNull();
    await expect
      .element(page.getByRole("checkbox", { name: "Individer 15+ 2018" }))
      .toBeVisible();
    await expect
      .element(page.getByRole("checkbox", { name: "Individer 16+ 2018" }))
      .toBeVisible();
  });

  it("identical curator names fall back to the variant slug, visibly and in the accessible name", async () => {
    await render(RepresentationPicker, {
      bands: variantFixture(["Individer", "Individer"]).bands,
      ...PROPS,
    });

    // Without the slug both rows read "Individer 2018" — the same choice twice.
    expect(texts(ROWS)).toEqual([
      "Individer individer-15plus 2018",
      "Individer individer-16plus 2018",
    ]);
    expect(texts(".col-row.nested .variant-key")).toEqual([
      "individer-15plus",
      "individer-16plus",
    ]);
    // Each checkbox is reachable BY NAME — the keyboard/screen-reader identity of
    // the two choices differs, not just their pixels.
    await expect
      .element(
        page.getByRole("checkbox", { name: "Individer individer-15plus 2018" }),
      )
      .toBeVisible();
    await expect
      .element(
        page.getByRole("checkbox", { name: "Individer individer-16plus 2018" }),
      )
      .toBeVisible();
    // The variant FILTER offers the same two choices, so its values carry the same
    // distinguisher rather than two chips reading "Individer".
    expect(texts(".dim-filters .ui-chip")).toEqual([
      "Individer individer-15plus",
      "Individer individer-16plus",
    ]);
  });

  it("stages the picked row's own variant when the two names are identical", async () => {
    const onapply = vi.fn();
    await render(RepresentationPicker, {
      bands: variantFixture(["Individer", "Individer"]).bands,
      ...PROPS,
      onapply,
    });

    await page
      .getByRole("checkbox", { name: "Individer individer-16plus 2018" })
      .click();
    await page
      .getByRole("button", {
        name: /Add to project|Remove from project|Apply changes/,
      })
      .click();
    expect(onapply).toHaveBeenCalledTimes(1);
    // Row-to-binding matching is unchanged: the label only got clearer.
    expect(onapply.mock.calls[0][0].adds).toHaveLength(1);
    expect(onapply.mock.calls[0][0].adds[0].row.variant).toBe(
      "individer-16plus",
    );
    expect(onapply.mock.calls[0][0].adds[0].row.column).toBe("Kon");
  });

  // ── The same choice in GRAPH mode (Y-14 rev 6) ────────────────────────────────
  // A graph cell reads as its column, coding and window — never as the POPULATION
  // delivering it — so two coexisting variants of one variable project onto two
  // separately selectable cells that read exactly alike, whatever the curator named
  // them. These fixtures are the ticket's shape with a window WIDE enough for the
  // graph to paint a cell's context (a one-year run floors to `CELL_MIN_W` and has
  // room for none), plus a successor lane so the succession context stays under test.
  const WIDE = { valid_from: "2000-01-01", valid_to: "2022-12-31" } as const;
  // Both populations deliver the column under ONE coding, so nothing but the
  // population itself can tell the two cells apart.
  const CODING = "SCB kön";
  const SYSS = "scb/rams/syss";

  function coexistingGraphFixture(
    labels: readonly string[],
    over: {
      period?: { valid_from: string; valid_to: string };
      edge?: Partial<GraphEdge>;
    } = {},
  ): {
    bands: PickerBand[];
    graph: RelationshipGraph;
  } {
    const period = over.period ?? WIDE;
    const base = variantFixture(labels, { period, coding: CODING });
    return {
      bands: [
        ...base.bands,
        {
          key: SYSS,
          name: "Sysselsättning",
          registerPrefix: "scb/rams",
          rows: [
            // A SHORT successor: four years of run floor to `CELL_MIN_W`, the narrowest
            // cell the graph draws, which is where the column chip has the least room
            // to paint itself (Y-68).
            row({
              column: "Syss",
              from: "2023-01-01",
              to: "2026-12-31",
              period: "2023 – 2026",
              windows: [{ from: "2023-01-01", to: "2026-12-31" }],
            }),
          ],
        } satisfies PickerBand,
      ],
      graph: graph({
        nodes: [
          ...base.graph.nodes,
          graphNode(SYSS, {
            label: "Sysselsättning",
            states: [
              graphState({
                state_id: "3",
                period_scope: "intervals",
                representation_run_id: 3,
                delivery_column_name: "Syss",
                valid_from: "2023-01-01",
                valid_to: "2026-12-31",
              }),
            ],
          }),
        ],
        edges: [edge("scb/lisa/kon", SYSS, over.edge)],
        focus_id: "scb/lisa/kon",
      }),
    };
  }

  /** Render that graph and assert, at every width the design language covers, that the
   * coexisting cells read differently, paint their distinguishing text in full, and
   * keep their own sub-row while the succession connector stays on the lanes. */
  async function expectGraphCellsIdentify(
    labels: readonly string[],
    populations: readonly string[],
    over: {
      period?: { valid_from: string; valid_to: string };
      window?: string;
    } = {},
  ): Promise<void> {
    await render(RepresentationPicker, {
      ...coexistingGraphFixture(labels, { period: over.period }),
      ...PROPS,
    });
    await graphPickerRendered();
    // Graph mode, not the list: the succession context the graph exists for survives
    // the fix.
    expect(document.querySelector(".col-list")).toBeNull();
    const cellText = (population: string) =>
      `Kon ${population} ${CODING} ${over.window ?? "2000 – 2022"}`;
    // No complete label may stand for two of the choices — the whole point.
    const expected = populations.map(cellText);
    expect(new Set(expected).size).toBe(expected.length);

    await atEveryWidth(async () => {
      await vi.waitFor(() => {
        expect(lanes()).toHaveLength(2);
        // Each cell names the population behind it, so no two selectable cells
        // read alike.
        expect(texts(".graph-cell", lanes()[0])).toEqual(expected);
      });
      const konLane = lanes()[0];
      const laneBox = konLane.getBoundingClientRect();
      const cells = [
        ...konLane.querySelectorAll<HTMLElement>(".graph-cell"),
      ].map((c) => ({ el: c, box: c.getBoundingClientRect() }));
      for (const [i, cell] of cells.entries()) {
        // The distinguishing text is painted WHOLE inside its cell — the cell
        // clips its overflow, so a text that doesn't fit would silently leave the
        // two cells identical again.
        const label = one<HTMLElement>(
          ".graph-cell-variant, .variant-key",
          cell.el,
        );
        const box = label.getBoundingClientRect();
        expect(label.textContent?.trim()).toBe(populations[i]);
        expect(label.scrollWidth).toBeLessThanOrEqual(label.clientWidth + 1);
        expect(box.left).toBeGreaterThanOrEqual(cell.box.left);
        expect(box.right).toBeLessThanOrEqual(cell.box.right);
        expect(box.top).toBeGreaterThanOrEqual(cell.box.top);
        expect(box.bottom).toBeLessThanOrEqual(cell.box.bottom);
        // …on its own sub-row, inside the lane it belongs to.
        expect(cell.box.top).toBeGreaterThanOrEqual(laneBox.top);
        expect(cell.box.bottom).toBeLessThanOrEqual(laneBox.bottom);
      }
      for (const [i, next] of cells.slice(1).entries()) {
        expect(cells[i].box.bottom).toBeLessThanOrEqual(next.box.top);
      }

      // Both choices are reachable BY NAME, so the two cells differ to a screen
      // reader and to the keyboard, not only in pixels.
      for (const population of populations) {
        await expect
          .element(page.getByRole("checkbox", { name: cellText(population) }))
          .toBeVisible();
      }
    });
  }

  it("names the population on graph cells two DISTINCT variants would otherwise share", async () => {
    await expectGraphCellsIdentify(DISTINCT, DISTINCT);
  });

  it("falls back to the variant slug on EVERY cell when a curator name IS another population's slug", async () => {
    // Three coexisting populations, one column, one coding, one window: the first two
    // share a name, so they show their slugs — and the third's curator name IS the
    // first's slug, so name-or-key decided per cell would paint two cells
    // "Kon individer-15plus since 2018". The whole collision group yields to the keys.
    await expectGraphCellsIdentify(NAME_IS_SLUG, [...VARIANTS], {
      period: { valid_from: "2018-01-01", valid_to: OPEN_ENDED_VALID_TO },
      window: "since 2018",
    });
    // All three read as the machine identity they are, so none is mistaken for a
    // curator's name.
    expect(texts(".graph-cell .variant-key")).toEqual([...VARIANTS]);
  });

  it("leaves the LIST rows of that three-population case as they already read", async () => {
    // The list projection is unambiguous for this fixture without any change: the two
    // rows a curator named alike carry their slug beside that name, and the third
    // shows the name it was given. The graph fix must not disturb it.
    await render(RepresentationPicker, {
      bands: variantFixture(NAME_IS_SLUG).bands,
      ...PROPS,
    });

    expect(texts(ROWS)).toEqual([
      "Individer individer-15plus 2018",
      "Individer individer-16plus 2018",
      "individer-15plus 2018",
    ]);
    expect(texts(".col-row.nested .variant-key")).toEqual([
      "individer-15plus",
      "individer-16plus",
    ]);
  });

  // One row: the second of two identically named populations is the hard case
  // (picking by name must reach its own variant, not the first match).
  it.each([
    {
      labels: ALIKE,
      population: "individer-16plus",
      variant: "individer-16plus",
    },
  ])(
    "stages the graph cell picked by its own name ($population)",
    async ({ labels, population, variant }) => {
      const onapply = vi.fn();
      await render(RepresentationPicker, {
        ...coexistingGraphFixture(labels),
        ...PROPS,
        onapply,
      });
      await graphPickerRendered();

      await page
        .getByRole("checkbox", {
          name: `Kon ${population} ${CODING} 2000 – 2022`,
        })
        .click();
      await page
        .getByRole("button", {
          name: /Add to project|Remove from project|Apply changes/,
        })
        .click();
      // The cell-to-row binding is untouched: the cell the user can finally tell
      // apart stages ITS OWN concrete variant and column.
      expect(onapply).toHaveBeenCalledTimes(1);
      expect(onapply.mock.calls[0][0].adds).toHaveLength(1);
      expect(onapply.mock.calls[0][0].adds[0].row.variant).toBe(variant);
      expect(onapply.mock.calls[0][0].adds[0].row.column).toBe("Kon");
    },
  );

  // ── The annotation the lane's own succession draws over those cells ───────────
  // A labelled edge floats over the track at the connector. With the population line
  // in them, the cells fill their whole 40px box, so an annotation drawn at the
  // midpoint of the two CELL centres paints over the first cell's column chip — the
  // reported regression. The annotation rides the band between the lanes instead.
  const ANNOTATION = {
    label: "Kon → Syss",
    effective_year: 2019,
  } satisfies Partial<GraphEdge>;

  it("keeps the lane's succession annotation clear of every cell it names", async () => {
    await render(RepresentationPicker, {
      ...coexistingGraphFixture(ALIKE, { edge: ANNOTATION }),
      ...PROPS,
    });
    await graphPickerRendered();
    await document.fonts.ready;

    await atEveryWidth(async () => {
      const reason = await vi.waitFor(() => {
        const el = one<HTMLElement>(".graph-reason");
        expect(el.getBoundingClientRect().width).toBeGreaterThan(0);
        return el;
      });
      // The annotation is the lane's real succession context, still rendered…
      expect(reason.textContent?.trim()).toBe("Kon → Syss · 2019");
      const box = reason.getBoundingClientRect();
      // …inside the timeline, and over NOTHING a cell has to show: its column
      // chip, the population identity beside the coding, and the period.
      const timeline =
        one<HTMLElement>(".graph-timeline").getBoundingClientRect();
      expect(box.top).toBeGreaterThanOrEqual(timeline.top);
      expect(box.bottom).toBeLessThanOrEqual(timeline.bottom);
      const painted = [
        ...document.querySelectorAll<HTMLElement>(
          ".graph-cell .col-chip, .graph-cell-context, .graph-cell-window",
        ),
      ];
      expect(painted.length).toBeGreaterThan(0);
      for (const part of painted) {
        expect(overlaps(box, part.getBoundingClientRect())).toBe(false);
      }
      // The cells it runs between still tile their lane in order.
      const cells = [
        ...lanes()[0].querySelectorAll<HTMLElement>(".graph-cell"),
      ].map((c) => c.getBoundingClientRect());
      expect(cells).toHaveLength(2);
      expect(cells[0].bottom).toBeLessThanOrEqual(cells[1].top);
    });
  });

  it("leaves graph mode when the curator names differ only past the cell's edge", async () => {
    // Two names that share a 40-character head and differ only in the last letter.
    // The identity text does not wrap or ellipsize, so a cell wide enough by the
    // fixed per-character budget the picker used to apply still clips both names to
    // the same visible prefix and paints them over the period. Only the browser,
    // measuring the real font, can tell — and here it says the graph cannot show
    // the choice, so the list does.
    const head = "W".repeat(40);
    const labels = [`${head}A`, `${head}B`];
    const variants = ["a", "b"];
    const period = {
      valid_from: "2000-01-01",
      valid_to: "2024-12-31",
    } as const;
    await render(RepresentationPicker, {
      bands: [
        {
          key: "scb/lisa/kon",
          name: "Kön",
          registerPrefix: "scb/lisa",
          rows: pickerRepresentations(
            labels.map((label, i) => ({
              state_id: String(i + 1),
              period_scope: "intervals",
              variant: variants[i],
              variant_label: label,
              delivery_column_name: "Kon",
              value_set_version_label: "",
              value_set_id: null,
              ...period,
            })),
          ),
        } satisfies PickerBand,
      ],
      graph: graph({
        nodes: [
          graphNode("scb/lisa/kon", {
            label: "Kön",
            states: labels.map((label, i) =>
              graphState({
                state_id: String(i + 1),
                period_scope: "intervals",
                representation_run_id: i + 1,
                variant: variants[i],
                variant_label: label,
                delivery_column_name: "Kon",
                ...period,
              }),
            ),
          }),
        ],
        edges: [],
        focus_id: "scb/lisa/kon",
      }),
      ...PROPS,
    });
    await document.fonts.ready;

    await vi.waitFor(() => {
      if (!document.querySelector(".col-list")) {
        throw new Error("list picker not rendered");
      }
    });
    expect(document.querySelector(".graph-picker")).toBeNull();
    // Both choices read to their distinguishing last letter, painted whole inside
    // their row — no ancestor clips them — and each carries the period.
    const rows = texts(ROWS);
    expect(rows).toEqual([`${head}A 2000 – 2024`, `${head}B 2000 – 2024`]);
    for (const el of document.querySelectorAll<HTMLElement>(ROWS)) {
      const box = el.getBoundingClientRect();
      for (const line of el.querySelectorAll<HTMLElement>("*")) {
        const lineBox = line.getBoundingClientRect();
        expect(line.scrollWidth).toBeLessThanOrEqual(line.clientWidth + 1);
        expect(lineBox.right).toBeLessThanOrEqual(box.right + 1);
      }
    }
  });

  // ── The column chip in a MINIMUM-WIDTH graph cell (Y-68) ──────────────────────
  // The Syss successor above runs four years, so its cell floors to `CELL_MIN_W` — the
  // narrowest cell the graph draws, and the least room the delivery-column chip ever
  // gets. One population here, so nothing but the fit is under test.
  const ONE_POPULATION = ["Individer 15+"] as const;

  /** Render the graph whose successor cell floors to `CELL_MIN_W`, then wait for it to
   * paint in the font the geometry below is measured in. */
  async function renderMinWidthCell(
    over: Partial<typeof PROPS> = {},
  ): Promise<void> {
    await render(RepresentationPicker, {
      ...coexistingGraphFixture(ONE_POPULATION),
      ...PROPS,
      ...over,
    });
    await graphPickerRendered();
    await document.fonts.ready;
  }

  /** The one selectable cell on lane `i` — lane 1 carries the minimum-width cell. */
  function cellOnLane(i: number): HTMLElement {
    return one<HTMLElement>("label.graph-cell", lanes()[i]);
  }

  it("paints a minimum-width cell's delivery column on ONE line inside its cell", async () => {
    await renderMinWidthCell();

    await atEveryWidth(async () => {
      await vi.waitFor(() => {
        expect(lanes()).toHaveLength(2);
      });
      // Still the succession graph the cell belongs to, not the list it falls back to.
      expect(document.querySelector(".col-list")).toBeNull();
      const cell = cellOnLane(1);
      const cellBox = cell.getBoundingClientRect();
      expect(cellBox.width).toBe(CELL_MIN_W);
      const chip = one<HTMLElement>(".col-chip", cell);
      expect(chip.textContent).toBe("Syss");
      // What the browser PAINTED, read off the text: one line box, not one per glyph…
      const lines = glyphLines(chip);
      expect(lines).toHaveLength(1);
      // …and every painted glyph inside the ancestor that clips it, top and bottom
      // included — the 40px-TALL cell is where the stacked first and last letters went.
      expect(lines[0].left).toBeGreaterThanOrEqual(cellBox.left);
      expect(lines[0].right).toBeLessThanOrEqual(cellBox.right);
      expect(lines[0].top).toBeGreaterThanOrEqual(cellBox.top);
      expect(lines[0].bottom).toBeLessThanOrEqual(cellBox.bottom);
      // The chip is not merely clipped INTO the cell: its pill fits the stack that
      // holds it, and the period that yielded the width starts clear of it.
      const chipBox = chip.getBoundingClientRect();
      const stack = one<HTMLElement>(".graph-cell-main", cell);
      expect(chipBox.right).toBeLessThanOrEqual(
        stack.getBoundingClientRect().right + 1,
      );
      const period = one<HTMLElement>(".graph-cell-window", cell);
      expect(period.getBoundingClientRect().left).toBeGreaterThanOrEqual(
        chipBox.right,
      );
      // The period truncates where it always did, with its description still whole.
      expect(period.textContent).toBe("2023 – 2026");
      expect(cell.title).toBe("Syss · 2023 – 2026");
    });
  });

  it("keeps the LIST row breaking a long column name the graph cell keeps whole", async () => {
    // The one-line rule is scoped to a graph cell. Outside it the shared chip still
    // breaks a long unbroken column anywhere, which is what a 375px list row needs.
    const column = "SYSSELSATTNINGSSTATUS_FOR_INDIVID_ARSMEDELVARDE";
    await render(RepresentationPicker, {
      bands: [
        {
          key: SYSS,
          name: "Sysselsättning",
          registerPrefix: "scb/rams",
          rows: [row({ column }), row({ column: "Syss", key: "v::Syss" })],
        } satisfies PickerBand,
      ],
      ...PROPS,
    });
    await document.fonts.ready;
    const chip = await vi.waitFor(() => {
      const found = [
        ...document.querySelectorAll<HTMLElement>(".col-list .col-chip"),
      ].find((c) => c.textContent === column);
      if (!found) {
        throw new Error(`missing list chip for ${column}`);
      }
      return found;
    });
    const listRow = chip.closest<HTMLElement>(".col-row");
    if (!listRow) {
      throw new Error("list chip outside a column row");
    }

    await atEveryWidth(async (width) => {
      const lines = glyphLines(chip);
      // However it breaks, it breaks INSIDE its row — the list never scrolls sideways.
      for (const line of lines) {
        expect(line.right).toBeLessThanOrEqual(
          listRow.getBoundingClientRect().right + 1,
        );
      }
      // …and at the narrowest width breaking is the only way it fits, which is the
      // wrapping the graph-cell rule must not have taken from the list.
      if (width === 375) {
        expect(lines.length).toBeGreaterThan(1);
      }
    });
  });

  it("keeps the LIST fallback when a delivery column outgrows its cell", async () => {
    // The stack keeping its column keeps it even where the column is wider than the
    // cell, so the identity under it can sit inside the stack and STILL be cut off by
    // the cell — two populations clipped to one visible prefix, the ambiguity the
    // geometry fallback exists to refuse. It reads the box that clips, not the stack
    // alone.
    const labels = [
      "Individer 15 ar och aldre A",
      "Individer 15 ar och aldre B",
    ];
    await render(RepresentationPicker, {
      ...variantFixture(labels, {
        coding: CODING,
        column: "SYSSELSATTNINGSSTATUS_FOR_INDIVID",
      }),
      ...PROPS,
    });

    await vi.waitFor(() => {
      if (!document.querySelector(".col-list")) {
        throw new Error("list picker not rendered");
      }
    });
    expect(document.querySelector(".graph-picker")).toBeNull();
    expect(texts(ROWS)).toEqual(labels.map((label) => `${label} 2018`));
  });
});
