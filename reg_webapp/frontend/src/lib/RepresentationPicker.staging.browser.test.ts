import { describe, expect, it, vi } from "vitest";
import { page } from "vitest/browser";
import { render } from "vitest-browser-svelte";
import {
  type PickerRepresentation,
  type PickerStateInput,
  pickerRepresentations,
} from "./catalog";
import {
  expectApplyDisabled,
  expectStagedAddColumnVisible,
} from "./picker-test-helpers";
import RepresentationPicker, {
  type PickerBand,
} from "./RepresentationPicker.svelte";
import {
  AXES,
  clickFilter,
  multiAxisBand,
  PROPS,
  row,
} from "./representation-picker-test-helpers";
import { type PickerCommittedRow, pickerRowKey } from "./staged_picker";

// Split from RepresentationPicker.browser.test.ts by contract surface: staging, apply and footer.
// Siblings: RepresentationPicker.{graph,graph-fallback,graph-history,labels,filters,staging,row-identity}.browser.test.ts.

type LisaIndividerVariant = "individer-16plus" | "individer-15plus";

function lisaIndividerState(variant: LisaIndividerVariant): PickerStateInput {
  const predecessor = variant === "individer-16plus";
  return {
    state_id: predecessor ? "1" : "2",
    period_scope: "intervals",
    variant,
    variant_label: predecessor ? "Individer, 16 plus" : "Individer, 15 plus",
    variant_family: "individer-15plus",
    variant_family_label: "Individer",
    delivery_column_name: "Kon",
    value_set_version_label: "",
    value_set_id: null,
    valid_from: predecessor ? "1990-01-01" : "2010-01-01",
    valid_to: predecessor ? "2009-12-31" : "2023-12-31",
  };
}

function lisaNarrowedBand(variant: LisaIndividerVariant): PickerBand {
  return {
    key: "scb/lisa/kon",
    name: "Kon",
    registerPrefix: "scb/lisa",
    rows: pickerRepresentations([lisaIndividerState(variant)]),
  };
}

function committedRowsFor(
  band: PickerBand,
  row: PickerRepresentation,
): Map<string, PickerCommittedRow> {
  const key = pickerRowKey(band, row);
  return new Map([
    [
      key,
      {
        key,
        registerVariant: `${band.registerPrefix}/${row.variant}`,
        variable: band.key,
        representation: row.representation,
        sourceName: "Source",
        sourcePeriod: 2000,
      },
    ],
  ]);
}

describe("RepresentationPicker dimension marking + filters (#908)", () => {
  it("keeps the 'Will be added' status in the staged-add checkbox accessible name while showing the compact '1 Column' tag (#1115 a11y)", async () => {
    await render(RepresentationPicker, {
      bands: [multiAxisBand()],
      axes: AXES,
      ...PROPS,
    });

    // Before staging, no row carries the pending-add status.
    await expect
      .element(page.getByRole("checkbox", { name: /Will be added/ }))
      .not.toBeInTheDocument();

    await page.getByRole("checkbox", { name: /DIN1/ }).click();

    // Visible tag stays the compact "1 Column" (the row-height fix), but the
    // checkbox's accessible name regains "Will be added" via the visually-hidden
    // prefix — the "+" glyph itself is aria-hidden, so without it a screen reader
    // would hear only the ambiguous "1 Column".
    await expectStagedAddColumnVisible();
    await expect
      .element(page.getByRole("checkbox", { name: /Will be added/ }))
      .toBeVisible();
  });

  it("keeps nested row height stable when staging a pick (#1127)", async () => {
    await render(RepresentationPicker, {
      bands: [multiAxisBand()],
      axes: AXES,
      ...PROPS,
    });

    const rowEl = await vi.waitFor(() => {
      const el = document.querySelector<HTMLElement>(
        ".col-row.nested .row-btn",
      );
      if (!el) {
        throw new Error("nested row not yet rendered");
      }
      return el;
    });
    const before = rowEl.getBoundingClientRect().height;

    rowEl.querySelector<HTMLInputElement>("input.cbox")?.click();

    await expectStagedAddColumnVisible();
    const after = rowEl.getBoundingClientRect().height;
    expect(after).toBeLessThanOrEqual(before + 1);
  });

  it("stages selectable superseded predecessor rows from the history disclosure (#926)", async () => {
    const onapply = vi.fn();
    const predecessor = {
      key: "scb/iot/dispink-old",
      name: "Disponibel inkomst familj",
      registerPrefix: "scb/iot",
      rows: [
        row({
          column: "DINFold",
          from: "1999-01-01",
          to: "2004-12-31",
          windows: [{ from: "1999-01-01", to: "2004-12-31" }],
          period: "1999 – 2004",
          wirePeriod: "1999..2004",
        }),
      ],
    } satisfies PickerBand;
    const successor = {
      key: "scb/iot/dispink-new",
      name: "Disponibel inkomst familj 2004",
      registerPrefix: "scb/iot",
      rows: [row({ column: "DINFnew" })],
      supersedes: [
        {
          name: predecessor.name,
          href: "/catalog/scb/iot/dispink-old",
          effectiveYear: 2005,
          band: predecessor,
        },
      ],
    } satisfies PickerBand;

    await render(RepresentationPicker, {
      bands: [successor],
      ...PROPS,
      onapply,
    });

    await page.getByText("supersedes 1 edition").click();
    await page.getByRole("checkbox", { name: /DINFold/ }).click();
    await expect.element(page.getByText("+1 column")).toBeVisible();
    await page.getByRole("button", { name: "Add to project" }).click();

    expect(onapply).toHaveBeenCalledTimes(1);
    const payload = onapply.mock.calls[0][0];
    expect(payload.adds).toHaveLength(1);
    expect(payload.adds[0].band.key).toBe("scb/iot/dispink-old");
    expect(payload.adds[0].row.column).toBe("DINFold");
  });

  it("deduplicates one folded predecessor shared by split successors (#926)", async () => {
    const onapply = vi.fn();
    const predecessor = {
      key: "scb/iot/dispink-old",
      name: "Disponibel inkomst old",
      registerPrefix: "scb/iot",
      rows: [row({ column: "DINFold" })],
    } satisfies PickerBand;
    const successors = ["new-a", "new-b"].map(
      (slug) =>
        ({
          key: `scb/iot/dispink-${slug}`,
          name: `Disponibel inkomst ${slug}`,
          registerPrefix: "scb/iot",
          rows: [row({ column: `DINF${slug}` })],
          supersedes: [
            {
              name: predecessor.name,
              href: "/catalog/scb/iot/dispink-old",
              effectiveYear: 2005,
              band: predecessor,
            },
          ],
        }) satisfies PickerBand,
    );

    await render(RepresentationPicker, {
      bands: successors,
      ...PROPS,
      onapply,
    });

    const summary = document.querySelector<HTMLElement>(
      "details.history summary",
    );
    if (!summary) {
      throw new Error("history disclosure not rendered");
    }
    summary.click();
    await page.getByRole("checkbox", { name: /DINFold/ }).click();
    await expect.element(page.getByText("+1 column")).toBeVisible();
    await page.getByRole("button", { name: "Add to project" }).click();

    expect(onapply).toHaveBeenCalledTimes(1);
    const payload = onapply.mock.calls[0][0];
    expect(payload.adds).toHaveLength(1);
    expect(payload.adds[0].band.key).toBe("scb/iot/dispink-old");
    expect(payload.adds[0].row.column).toBe("DINFold");
  });

  it("applies filters and hidden-counts to folded history rows (#926)", async () => {
    const predecessor = {
      key: "scb/iot/dispink-old",
      name: "Disponibel inkomst old",
      registerPrefix: "scb/iot",
      rows: [row({ column: "DINFold" })],
      facetsByColumn: {
        DINFold: [{ axis: "era", value: "old", label: "Old level" }],
      },
    } satisfies PickerBand;
    const successor = {
      key: "scb/iot/dispink-new",
      name: "Disponibel inkomst new",
      registerPrefix: "scb/iot",
      rows: [row({ column: "DINFnew" })],
      facetsByColumn: {
        DINFnew: [{ axis: "era", value: "new", label: "New level" }],
      },
      supersedes: [
        {
          name: predecessor.name,
          href: "/catalog/scb/iot/dispink-old",
          effectiveYear: 2005,
          band: predecessor,
        },
      ],
    } satisfies PickerBand;

    await render(RepresentationPicker, {
      bands: [successor],
      axes: [{ name: "era", label: "Era" }],
      ...PROPS,
    });

    await page.getByText("supersedes 1 edition").click();
    await page.getByRole("checkbox", { name: /DINFold/ }).click();
    await expect.element(page.getByText("+1 column")).toBeVisible();

    clickFilter("Old level");
    await expect
      .element(page.getByText("Showing 1 of 2 columns"))
      .toBeVisible();
    await expect.element(page.getByText("+1 column")).toBeVisible();
    await expect
      .element(page.getByText("+1 column (1 hidden by filters)"))
      .not.toBeInTheDocument();
    const details =
      document.querySelector<HTMLDetailsElement>("details.history");
    if (!details) {
      throw new Error("history disclosure not rendered");
    }
    details.open = true;
    await expect
      .element(page.getByRole("checkbox", { name: /DINFold/ }))
      .toBeVisible();

    await page.getByRole("button", { name: "Clear filters" }).click();
    clickFilter("New level");
    await expect
      .element(page.getByText("+1 column (1 hidden by filters)"))
      .toBeVisible();
    await expect
      .element(page.getByRole("checkbox", { name: /DINFold/ }))
      .not.toBeInTheDocument();
  });

  it("shows global select-all for one successor plus one folded predecessor (#926)", async () => {
    const onapply = vi.fn();
    const predecessor = {
      key: "scb/iot/dispink-old",
      name: "Disponibel inkomst old",
      registerPrefix: "scb/iot",
      rows: [row({ column: "DINFold" })],
    } satisfies PickerBand;
    const successor = {
      key: "scb/iot/dispink-new",
      name: "Disponibel inkomst new",
      registerPrefix: "scb/iot",
      rows: [row({ column: "DINFnew" })],
      supersedes: [
        {
          name: predecessor.name,
          href: "/catalog/scb/iot/dispink-old",
          effectiveYear: 2005,
          band: predecessor,
        },
      ],
    } satisfies PickerBand;

    await render(RepresentationPicker, {
      bands: [successor],
      ...PROPS,
      onapply,
    });

    await page.getByRole("checkbox", { name: "Select all columns" }).click();
    await expect.element(page.getByText("+2 columns")).toBeVisible();

    const details =
      document.querySelector<HTMLDetailsElement>("details.history");
    if (!details) {
      throw new Error("history disclosure not rendered");
    }
    details.open = true;
    await expect
      .element(page.getByRole("checkbox", { name: /DINFold/ }))
      .toBeChecked();

    await page.getByRole("button", { name: "Add to project" }).click();
    expect(onapply).toHaveBeenCalledTimes(1);
    const addedColumns = onapply.mock.calls[0][0].adds.map(
      (selection: { row: PickerRepresentation }) => selection.row.column,
    );
    expect(addedColumns.sort()).toEqual(["DINFnew", "DINFold"]);
  });

  it("hides global select-all when filters leave one folded family band visible (#926)", async () => {
    const predecessor = {
      key: "scb/iot/dispink-old",
      name: "Disponibel inkomst old",
      registerPrefix: "scb/iot",
      rows: [row({ column: "DINFold" })],
      facetsByColumn: {
        DINFold: [{ axis: "era", value: "old", label: "Old level" }],
      },
    } satisfies PickerBand;
    const successor = {
      key: "scb/iot/dispink-new",
      name: "Disponibel inkomst new",
      registerPrefix: "scb/iot",
      rows: [row({ column: "DINFnew1" }), row({ column: "DINFnew2" })],
      facetsByColumn: {
        DINFnew1: [{ axis: "era", value: "new", label: "New level" }],
        DINFnew2: [{ axis: "era", value: "new", label: "New level" }],
      },
      supersedes: [
        {
          name: predecessor.name,
          href: "/catalog/scb/iot/dispink-old",
          effectiveYear: 2005,
          band: predecessor,
        },
      ],
    } satisfies PickerBand;

    await render(RepresentationPicker, {
      bands: [successor],
      axes: [{ name: "era", label: "Era" }],
      ...PROPS,
    });

    await vi.waitFor(() => {
      if (
        !document.querySelector(
          '.select-all-row input[aria-label="Select all columns"]',
        )
      ) {
        throw new Error("global select-all not rendered");
      }
    });
    clickFilter("New level");
    await expect
      .element(page.getByText("Showing 2 of 3 columns"))
      .toBeVisible();
    expect(
      document.querySelector(
        '.select-all-row input[aria-label="Select all columns"]',
      ),
    ).toBeNull();
  });

  it("clears staged adds when a narrowed folded variant changes concrete segment", async () => {
    const onapply = vi.fn();
    const { rerender } = await render(RepresentationPicker, {
      bands: [lisaNarrowedBand("individer-16plus")],
      ...PROPS,
      onapply,
    });

    await page.getByRole("checkbox", { name: /Kon/ }).click();
    await expect.element(page.getByText("+1 column")).toBeVisible();

    await rerender({
      bands: [lisaNarrowedBand("individer-15plus")],
      ...PROPS,
      onapply,
    });

    await expect.element(page.getByText("+1 column")).not.toBeInTheDocument();
    await expectApplyDisabled();
    expect(onapply).not.toHaveBeenCalled();
  });

  it("freezes staging controls while Apply is pending", async () => {
    let finishApply: () => void = () => {
      throw new Error("apply did not start");
    };
    let started!: () => void;
    const applyStarted = new Promise<void>((resolve) => {
      started = resolve;
    });
    const onapply = vi.fn().mockImplementation(
      () =>
        new Promise<void>((resolve) => {
          finishApply = resolve;
          started();
        }),
    );
    await render(RepresentationPicker, {
      bands: [multiAxisBand()],
      axes: AXES,
      ...PROPS,
      onapply,
    });

    await page.getByRole("checkbox", { name: /DIN1/ }).click();
    await expectStagedAddColumnVisible();
    await page
      .getByRole("button", {
        name: /Add to project|Remove from project|Apply changes/,
      })
      .click();
    await applyStarted;

    await expect
      .element(page.getByRole("checkbox", { name: /DIN2/ }))
      .toBeDisabled();
    await expect
      .element(page.getByRole("checkbox", { name: /Select all columns of/ }))
      .toBeDisabled();
    await expect
      .element(page.getByRole("button", { name: "Reset" }))
      .toBeDisabled();

    finishApply();
    await expect
      .element(page.getByText("No staged changes"))
      .not.toBeInTheDocument();
    await expectApplyDisabled();
  });

  it("retracts its staged report when it unmounts", async () => {
    // The host reads this report to keep its page-level status (an applied
    // confirmation, a refused-apply notice) from sitting beside staging that
    // contradicts it. This picker is conditional — a browse narrowing that leaves
    // the page with no rows unmounts it — so a report only ever raised while
    // mounted would latch that status on with nothing left to reset it.
    const onstagechange = vi.fn();
    const view = await render(RepresentationPicker, {
      bands: [multiAxisBand()],
      axes: AXES,
      ...PROPS,
      onstagechange,
    });

    await page.getByRole("checkbox", { name: /DIN1/ }).click();
    await expect.poll(() => onstagechange.mock.calls.at(-1)?.[0]).toBe(true);

    view.unmount();
    expect(onstagechange.mock.calls.at(-1)?.[0]).toBe(false);
  });

  it("does not stage period-only source changes from a partial picker", async () => {
    const onapply = vi.fn();
    const band = multiAxisBand();
    const committedRows = committedRowsFor(band, band.rows[0]);
    await render(RepresentationPicker, {
      bands: [band],
      axes: AXES,
      ...PROPS,
      activePeriod: "2001",
      committedRows,
      onapply,
    });

    await expect
      .element(page.getByText("No staged changes"))
      .not.toBeInTheDocument();
    await expect
      .element(page.getByRole("button", { name: "Reset" }))
      .not.toBeInTheDocument();
    await expectApplyDisabled();

    expect(onapply).not.toHaveBeenCalled();
  });

  it("allows remove-only applies before add seed context is ready", async () => {
    const onapply = vi.fn();
    const band = multiAxisBand();
    const committedRows = committedRowsFor(band, band.rows[0]);
    await render(RepresentationPicker, {
      bands: [band],
      axes: AXES,
      ...PROPS,
      canAdd: false,
      committedRows,
      onapply,
    });

    const rowCheckbox = page.getByRole("checkbox", { name: /DIN1/ });
    await expect.element(rowCheckbox).toBeChecked();
    await rowCheckbox.click();
    await expect.element(page.getByText("Will be removed")).toBeVisible();
    const apply = page.getByRole("button", { name: "Remove from project" });
    await expect.element(apply).toBeEnabled();
    await apply.click();

    expect(onapply).toHaveBeenCalledTimes(1);
    expect(onapply.mock.calls[0][0].removes).toHaveLength(1);
  });

  it("labels staged footer actions by diff shape", async () => {
    const band = multiAxisBand();
    const committedRows = committedRowsFor(band, band.rows[0]);
    await render(RepresentationPicker, {
      bands: [band],
      axes: AXES,
      ...PROPS,
      committedRows,
    });

    await expectApplyDisabled();

    await page.getByRole("checkbox", { name: /DIN2/ }).click();
    await expect
      .element(page.getByRole("button", { name: "Add to project" }))
      .toBeVisible();

    await page.getByRole("checkbox", { name: /DIN1/ }).click();
    await expect
      .element(page.getByRole("button", { name: "Apply changes" }))
      .toBeVisible();
  });

  it("allows committed nonselectable rows to be removed without allowing new adds", async () => {
    const onapply = vi.fn();
    const base = multiAxisBand();
    const nonselectableCommitted = {
      ...base.rows[0],
      selectable: false,
    };
    const nonselectableUncommitted = {
      ...base.rows[1],
      selectable: false,
    };
    const band = {
      ...base,
      rows: [nonselectableCommitted, nonselectableUncommitted, base.rows[2]],
    };
    const committedRows = committedRowsFor(band, nonselectableCommitted);
    await render(RepresentationPicker, {
      bands: [band],
      axes: AXES,
      ...PROPS,
      committedRows,
      onapply,
    });

    const committedCheckbox = page.getByRole("checkbox", { name: /DIN1/ });
    await expect.element(committedCheckbox).toBeChecked();
    await expect.element(committedCheckbox).toBeEnabled();
    await expect
      .element(page.getByRole("checkbox", { name: /DIN2/ }))
      .toBeDisabled();

    await committedCheckbox.click();
    await expect.element(page.getByText("Will be removed")).toBeVisible();
    const apply = page.getByRole("button", {
      name: /Add to project|Remove from project|Apply changes/,
    });
    await expect.element(apply).toBeEnabled();
    await apply.click();

    expect(onapply).toHaveBeenCalledTimes(1);
    expect(onapply.mock.calls[0][0].removes).toHaveLength(1);
    expect(onapply.mock.calls[0][0].adds).toHaveLength(0);
  });

  it("stages all rows backed by the same null binding when one is removed", async () => {
    const onapply = vi.fn();
    const band = multiAxisBand();
    const committedRows = new Map<string, PickerCommittedRow>(
      band.rows.slice(0, 2).map((r) => {
        const key = pickerRowKey(band, r);
        return [
          key,
          {
            key,
            registerVariant: `${band.registerPrefix}/${r.variant}`,
            variable: band.key,
            representation: null,
            sourceName: "Source",
            sourcePeriod: 2020,
          },
        ];
      }),
    );
    await render(RepresentationPicker, {
      bands: [band],
      axes: AXES,
      ...PROPS,
      committedRows,
      onapply,
    });

    await page.getByRole("checkbox", { name: /DIN1/ }).click();

    await expect.element(page.getByText("-2 columns")).toBeVisible();
    expect(
      document.querySelectorAll(".col-list .row-btn.staged-remove"),
    ).toHaveLength(2);
    await page
      .getByRole("button", {
        name: /Add to project|Remove from project|Apply changes/,
      })
      .click();
    expect(onapply).toHaveBeenCalledTimes(1);
    expect(onapply.mock.calls[0][0].removes).toHaveLength(2);
  });
});

describe("RepresentationPicker footer + row-height stability (#1115)", () => {
  it("does not grow a single-column row's height when it is picked", async () => {
    // The #1115 regression: a Tag first appearing inline in the row grew the
    // row (Tag `line-height: 1.4` vs the row's centered 0.9rem primary). Its
    // height must be pixel-identical before and after staging.
    await render(RepresentationPicker, {
      bands: [lisaNarrowedBand("individer-16plus")],
      ...PROPS,
    });

    const row = () =>
      document.querySelector(".col-row.single .row-btn") as HTMLElement;
    const before = row().clientHeight;

    await page.getByRole("checkbox", { name: /Kon/ }).click();
    await expectStagedAddColumnVisible();

    expect(row().clientHeight).toBe(before);
  });
});
