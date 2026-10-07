import { describe, expect, it, vi } from "vitest";
import { page } from "vitest/browser";
import { render } from "vitest-browser-svelte";
import RepresentationPicker, {
  type PickerBand,
} from "./RepresentationPicker.svelte";
import {
  edge,
  graph,
  graphNode,
  graphState,
  PROPS,
  row,
} from "./representation-picker-test-helpers";

// Split from RepresentationPicker.browser.test.ts by contract surface: graph succession history, renamed lanes and folded variant families.
// Siblings: RepresentationPicker.{graph,graph-fallback,graph-history,labels,filters,staging,row-identity}.browser.test.ts.

describe("RepresentationPicker graph mode (#904)", () => {
  it("draws round-trip representation edges to the resumed later cell", async () => {
    const aFqid = "scb/lisa/roundtrip";
    await render(RepresentationPicker, {
      bands: [
        {
          key: aFqid,
          name: "Roundtrip leaf",
          registerPrefix: "scb/lisa",
          rows: [
            row({
              column: "BorgNr",
              representation: null,
              renamedColumns: ["PersOrgNr"],
              from: "2007-01-01",
              to: "2023-12-31",
              windows: [
                { from: "2007-01-01", to: "2013-12-31" },
                { from: "2014-01-01", to: "2017-12-31" },
                { from: "2018-01-01", to: "2023-12-31" },
              ],
              period: "2007 – 2023",
            }),
          ],
        } satisfies PickerBand,
      ],
      graph: graph({
        nodes: [
          graphNode(aFqid, {
            states: [
              graphState({
                delivery_column_name: "BorgNr",
                valid_from: "2007-01-01",
                valid_to: "2013-12-31",
              }),
              graphState({
                state_id: "2",
                period_scope: "intervals",
                representation_run_id: 2,
                delivery_column_name: "PersOrgNr",
                valid_from: "2014-01-01",
                valid_to: "2017-12-31",
              }),
              graphState({
                state_id: "3",
                period_scope: "intervals",
                representation_run_id: 3,
                delivery_column_name: "BorgNr",
                valid_from: "2018-01-01",
                valid_to: "2023-12-31",
              }),
            ],
          }),
        ],
        edges: [
          {
            id: "repr-borgnr-persorgnr",
            kind: "succession",
            source: aFqid,
            target: aFqid,
            label: null,
            effective_year: 2014,
            source_column: "BorgNr",
            target_column: "PersOrgNr",
            variant: null,
          },
          {
            id: "repr-persorgnr-borgnr",
            kind: "succession",
            source: aFqid,
            target: aFqid,
            label: null,
            effective_year: 2018,
            source_column: "PersOrgNr",
            target_column: "BorgNr",
            variant: null,
          },
        ],
        focus_id: aFqid,
      }),
      ...PROPS,
    });

    const line = await vi.waitFor(() => {
      const edge = document.querySelector<SVGLineElement>(
        '.graph-edge.representation[data-edge-id="repr-persorgnr-borgnr"]',
      );
      if (!edge) {
        throw new Error("round-trip representation edge not rendered");
      }
      return edge;
    });
    const x1 = Number(line.getAttribute("x1"));
    const x2 = Number(line.getAttribute("x2"));
    expect(x2).toBeGreaterThan(x1);
    expect(document.querySelector(".graph-picker")?.textContent).toContain(
      "PersOrgNr → BorgNr · 2018",
    );
    const labelPositions = await vi.waitFor(() => {
      const labels = [
        ...document.querySelectorAll<HTMLElement>(".graph-reason"),
      ].filter(
        (label) =>
          label.textContent?.includes("BorgNr → PersOrgNr · 2014") ||
          label.textContent?.includes("PersOrgNr → BorgNr · 2018"),
      );
      if (labels.length !== 2) {
        throw new Error("round-trip labels not rendered");
      }
      return labels.map((label) => `${label.style.left}:${label.style.top}`);
    });
    expect(new Set(labelPositions).size).toBe(2);
  });

  it("marks dead renamed predecessor lanes with the leaf slug and renamed hint", async () => {
    const liveFqid = "scb/lisa/sni2007";
    const deadFqid = "scb/lisa/sni92";
    await render(RepresentationPicker, {
      bands: [
        {
          key: liveFqid,
          name: "Näringsgren (SNI 2007)",
          registerPrefix: "scb/lisa",
          rows: [row({ column: "SNI2007" })],
        } satisfies PickerBand,
      ],
      graph: graph({
        nodes: [
          graphNode(liveFqid, {
            id: "live",
            fqid: liveFqid,
            label: "Näringsgren (SNI 2007)",
            group_key: null,
            group_label: null,
            states: [graphState({ delivery_column_name: "SNI2007" })],
          }),
          graphNode(deadFqid, {
            id: "dead",
            fqid: deadFqid,
            label: "Näringsgren (SNI 92)",
            group_key: null,
            group_label: null,
            states: [],
          }),
        ],
        edges: [
          {
            id: "dead->live",
            kind: "succession",
            source: "dead",
            target: "live",
            label: null,
          },
        ],
        focus_id: "live",
      }),
      ...PROPS,
    });

    const renamedLink = await vi.waitFor(() => {
      const el = [...document.querySelectorAll(".graph-name")].find(
        (node) =>
          (node as HTMLAnchorElement).getAttribute("href") ===
          "/catalog/scb/lisa/sni92",
      );
      if (!el) {
        throw new Error("renamed predecessor link not rendered");
      }
      return el as HTMLAnchorElement;
    });
    expect(renamedLink.textContent?.replace(/\s+/g, " ").trim()).toContain(
      "sni92 (renamed)",
    );
    expect(
      renamedLink.closest(".graph-lane")?.classList.contains("muted"),
    ).toBe(true);
    expect(document.body.textContent ?? "").toContain(
      "renamed predecessor with no live states",
    );
  });

  it("dims folded graph cells by the cell's era, not the folded row span", async () => {
    const onapply = vi.fn();
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
              from: "1990-01-01",
              to: "2020-12-31",
              windows: [
                { from: "1990-01-01", to: "1999-12-31" },
                { from: "2000-01-01", to: "2020-12-31" },
              ],
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
                valid_from: "1990-01-01",
                valid_to: "1999-12-31",
              }),
              graphState({
                state_id: "2",
                period_scope: "intervals",
                representation_run_id: 2,
                delivery_column_name: "NEW",
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
        focus_id: aFqid,
      }),
      ...PROPS,
      window: [2010, 2010],
      onapply,
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
    const newCell = [
      ...document.querySelectorAll<HTMLElement>(".graph-cell"),
    ].find((el) => el.textContent?.includes("NEW"));
    expect(oldCell.classList.contains("dimmed")).toBe(true);
    expect(newCell?.classList.contains("dimmed")).toBe(false);

    await page.getByRole("checkbox", { name: /^OLD\b/ }).click();
    await page
      .getByRole("button", {
        name: /Add to project|Remove from project|Apply changes/,
      })
      .click();
    expect(onapply).toHaveBeenCalledTimes(1);
    expect(onapply.mock.calls[0][0].adds[0].row.column).toBe("NEW");
  });

  it("labels a graph-matched folded variant-family row with its full family period", async () => {
    const predecessorFqid = "scb/lisa/kon-old";
    const currentFqid = "scb/lisa/kon";
    await render(RepresentationPicker, {
      bands: [
        {
          key: currentFqid,
          name: "Kön",
          registerPrefix: "scb/lisa",
          rows: [
            row({
              key: "individer-15plus::Kon",
              variant: "individer-15plus",
              variantLabel: "Individer, 15 år och äldre",
              variantFamily: "individer-15plus",
              variantFamilyLabel: "Individer",
              column: "Kon",
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
          graphNode(predecessorFqid, {
            label: "Kön old",
            states: [
              graphState({
                variant: "individer-16plus",
                delivery_column_name: "Kon",
                valid_from: "1990-01-01",
                valid_to: "2009-12-31",
              }),
            ],
          }),
          graphNode(currentFqid, {
            label: "Kön",
            states: [
              graphState({
                variant: "individer-15plus",
                delivery_column_name: "Kon",
                valid_from: "2010-01-01",
                valid_to: "2023-12-31",
              }),
            ],
          }),
        ],
        edges: [edge(predecessorFqid, currentFqid)],
        focus_id: currentFqid,
      }),
      ...PROPS,
    });

    await expect
      .element(page.getByRole("checkbox", { name: /Kon.*1990.*2023/ }))
      .toBeVisible();
    const matchedCell = await vi.waitFor(() => {
      const cell = [
        ...document.querySelectorAll<HTMLLabelElement>("label.graph-cell"),
      ].find((el) => el.textContent?.includes("Kon"));
      if (!cell) {
        throw new Error("matched graph cell not rendered");
      }
      return cell;
    });
    expect(matchedCell.textContent).toContain("1990 – 2023");
    expect(matchedCell.textContent).not.toContain("2010 – 2023");
  });

  it("matches every concrete variant graph cell for a folded family row", async () => {
    const onapply = vi.fn();
    const fqid = "scb/lisa/kon";
    await render(RepresentationPicker, {
      bands: [
        {
          key: fqid,
          name: "Kön",
          registerPrefix: "scb/lisa",
          rows: [
            row({
              key: "individer-15plus::Kon",
              variant: "individer-15plus",
              variantLabel: "Individer, 15 år och äldre",
              variantFamily: "individer-15plus",
              variantFamilyLabel: "Individer",
              column: "Kon",
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
                variant: "individer-16plus",
                representation_run_id: 1,
                delivery_column_name: "Kon",
                valid_from: "1990-01-01",
                valid_to: "2009-12-31",
              }),
              graphState({
                state_id: "2",
                period_scope: "intervals",
                variant: "individer-15plus",
                representation_run_id: 2,
                delivery_column_name: "Kon",
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
      onapply,
    });

    const matchedCells = await vi.waitFor(() => {
      const cells = [
        ...document.querySelectorAll<HTMLLabelElement>("label.graph-cell"),
      ].filter((el) => el.textContent?.includes("1990 – 2023"));
      if (cells.length !== 2) {
        throw new Error(
          `expected two matched graph cells, got ${cells.length}`,
        );
      }
      return cells;
    });
    for (const cell of matchedCells) {
      expect(cell.textContent).toContain("1990 – 2023");
    }

    matchedCells[0].querySelector("input")?.click();

    await expect.element(page.getByText("+1 column")).toBeVisible();
    await page
      .getByRole("button", {
        name: /Add to project|Remove from project|Apply changes/,
      })
      .click();
    expect(onapply).toHaveBeenCalledTimes(1);
    expect(onapply.mock.calls[0][0].adds[0].row.column).toBe("Kon");
  });

  it("keeps predecessor variant cells in a folded family group graph projection", async () => {
    const familyFqid = "scb/lisa/kon";
    const successorFqid = "scb/lisa/kon-next";
    await render(RepresentationPicker, {
      bands: [
        {
          key: familyFqid,
          name: "Kön",
          registerPrefix: "scb/lisa",
          href: "/catalog/scb/lisa/kon",
          rows: [
            row({
              key: "individer-15plus::Kon",
              variant: "individer-15plus",
              variantLabel: "Individer, 15 år och äldre",
              variantFamily: "individer-15plus",
              variantFamilyLabel: "Individer",
              column: "Kon",
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
        {
          key: successorFqid,
          name: "Successor",
          registerPrefix: "scb/lisa",
          href: "/catalog/scb/lisa/kon-next",
          rows: [row({ column: "Kon2" })],
        } satisfies PickerBand,
      ],
      graphMemberHrefs: {
        [familyFqid]: "/catalog/scb/lisa/kon",
        [successorFqid]: "/catalog/scb/lisa/kon-next",
      },
      graph: graph({
        nodes: [
          graphNode(familyFqid, {
            states: [
              graphState({
                variant: "individer-16plus",
                representation_run_id: 1,
                delivery_column_name: "Kon",
                valid_from: "1990-01-01",
                valid_to: "2009-12-31",
              }),
              graphState({
                state_id: "2",
                period_scope: "intervals",
                variant: "individer-15plus",
                representation_run_id: 2,
                delivery_column_name: "Kon",
                valid_from: "2010-01-01",
                valid_to: "2023-12-31",
              }),
            ],
          }),
          graphNode(successorFqid, {
            states: [
              graphState({
                state_id: "3",
                period_scope: "intervals",
                representation_run_id: 3,
                delivery_column_name: "Kon2",
                valid_from: "2024-01-01",
                valid_to: "9999-12-31",
              }),
            ],
          }),
        ],
        edges: [edge(familyFqid, successorFqid)],
        focus_id: familyFqid,
      }),
      ...PROPS,
    });

    const matchedCells = await vi.waitFor(() => {
      const cells = [
        ...document.querySelectorAll<HTMLLabelElement>("label.graph-cell"),
      ].filter((el) => el.textContent?.includes("1990 – 2023"));
      if (cells.length !== 2) {
        throw new Error(
          `expected two folded-family graph cells, got ${cells.length}`,
        );
      }
      return cells;
    });
    for (const cell of matchedCells) {
      expect(cell.textContent).toContain("1990 – 2023");
    }
    expect(document.querySelectorAll(".graph-edge")).toHaveLength(1);
    expect(document.querySelector(".col-list")).toBeNull();
  });

  it("keeps an open-start graph cell in-window before the finite graph floor", async () => {
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
              column: "OPEN",
              from: "0001-01-01",
              to: "1999-12-31",
              windows: [{ from: "0001-01-01", to: "1999-12-31" }],
              period: "until 1999",
              wirePeriod: null,
            }),
          ],
        } satisfies PickerBand,
        {
          key: bFqid,
          name: "B",
          registerPrefix: "scb/lisa",
          rows: [row({ column: "NEXT" })],
        } satisfies PickerBand,
      ],
      graph: graph({
        nodes: [
          graphNode(aFqid, {
            states: [
              graphState({
                delivery_column_name: "OPEN",
                valid_from: null,
                valid_to: "1999-12-31",
              }),
            ],
          }),
          graphNode(bFqid, {
            states: [
              graphState({
                state_id: "2",
                period_scope: "intervals",
                representation_run_id: 2,
                delivery_column_name: "NEXT",
                valid_from: "2000-01-01",
                valid_to: "2020-12-31",
              }),
            ],
          }),
        ],
        edges: [edge(aFqid, bFqid)],
        focus_id: aFqid,
      }),
      ...PROPS,
      window: [1980, 1980],
    });

    const openCell = await vi.waitFor(() => {
      const cell = [
        ...document.querySelectorAll<HTMLElement>(".graph-cell"),
      ].find((el) => el.textContent?.includes("OPEN"));
      if (!cell) {
        throw new Error("OPEN cell not rendered");
      }
      return cell;
    });
    expect(openCell.classList.contains("dimmed")).toBe(false);
  });
});
