import { beforeEach, describe, expect, it, vi } from "vitest";
import { page } from "vitest/browser";
import { render } from "vitest-browser-svelte";
import { getValues } from "./api";
import ValueSetView from "./ValueSetView.svelte";
import {
  classState,
  coding,
  FQID,
  normalizedText,
  plainState,
  serveValues,
  state,
} from "./value-set-view-test-helpers";

// The pure value-set / coding viewer (#905 — extracted from the retired StatesView,
// #668 dogfooding M13/M18/M20). The kommun shape blows up by VINTAGE (415 states);
// the view dedups at TWO levels — classification editions by `classification_slug`,
// others by `value_set_id` — and shows DISTINCT value sets, classification ones
// linking out (no code dump). A FilterInput + per-row Isolate replace the old
// all-chips strip. A `?codes=<column>` deep link (the picker's "codings vary"
// nudge) seeds the isolation via `focusColumn`. Single-state detail + the empty
// mode are unchanged. NO resolution (variant / value-set version) plumbing — the
// picker owns that now.

// Y-46: the states carry each coding's IDENTITY and a cardinality-independent
// summary, never its members — so the code tables here are BOUNDED READS. The
// stub below serves them out of a per-test registry, filtering and windowing in
// the same order the route does (filter the whole set, then page it).
vi.mock("./api", async (importOriginal) => {
  const actual = await importOriginal<typeof import("./api")>();
  return { ...actual, getValues: vi.fn() };
});

beforeEach(() => {
  vi.mocked(getValues).mockReset();
  vi.mocked(getValues).mockImplementation(serveValues);
});

const ageState = state({
  state_id: "5",
  value_set_id: "500",
  classifications: [],
  value_set_version_label: "Ålder",
  variant: "personer",
  valid_from: "2000-01-01",
  valid_to: "2000-12-31",
  value_set_summary: coding(
    500,
    Array.from({ length: 21 }, (_, age) => ({
      code: String(age),
      label: `${age} år`,
    })),
    { integerRange: { min: 0, max: 20 } },
  ),
});

describe("ValueSetView — value-set-centric multi-state view (#668/#905)", () => {
  it.each([false, true])(
    "shows literal names, descriptions, definitions and units (multiple=%s)",
    async (multiple) => {
      const first = state({
        state_id: "10",
        value_set_id: "500",
        delivery_column_name: "AGI1LonFink01",
        name: "Income reported in January",
        description: "Exact January source description",
        definition: "Löneinkomst i januari från största förvärvskällan",
        measurement_unit: "100-tals kronor",
      });
      const states = multiple
        ? [
            first,
            state({
              ...first,
              delivery_column_name: "AGI1LonFink02",
              name: "Income reported in February",
              description: "Exact February source description",
              definition: "Löneinkomst i februari från största förvärvskällan",
              measurement_unit: "Kronor (SEK)",
            }),
          ]
        : [first];
      await render(ValueSetView, { fqid: FQID, states, narrowed: false });
      await expect
        .element(
          page.getByText("Löneinkomst i januari från största förvärvskällan"),
        )
        .toBeVisible();
      await expect.element(page.getByText("100-tals kronor")).toBeVisible();
      await expect
        .element(page.getByText("Income reported in January"))
        .toBeVisible();
      await expect
        .element(page.getByText("Exact January source description"))
        .toBeVisible();
      if (multiple) {
        await expect
          .element(
            page.getByText(
              "Löneinkomst i februari från största förvärvskällan",
            ),
          )
          .toBeVisible();
        await expect.element(page.getByText("Kronor (SEK)")).toBeVisible();
        await expect
          .element(page.getByText("Income reported in February"))
          .toBeVisible();
        await expect
          .element(page.getByText("Exact February source description"))
          .toBeVisible();
      }
    },
  );

  it("omits common facts and does not fill absent delivery facts", async () => {
    await render(ValueSetView, {
      fqid: FQID,
      states: [
        state({
          name: "Common name",
          description: "Common description",
          definition: "Common definition",
          measurement_unit: "Kronor",
        }),
        state({
          state_id: "2",
          name: null,
          description: null,
          definition: null,
          measurement_unit: null,
        }),
      ],
      narrowed: false,
      commonName: "Common name",
      commonDescription: "Common description",
      commonDefinition: "Common definition",
      commonUnit: "Kronor",
    });
    expect(document.querySelector(".state-definitions")).toBeNull();
    expect(document.body.textContent).not.toContain("Common name");
    expect(document.body.textContent).not.toContain("Common description");
    expect(document.body.textContent).not.toContain("Common definition");
    expect(document.body.textContent).not.toContain("Kronor");
  });

  it("renders expanded state definitions with duplicate source state ids (#736)", async () => {
    const states = [
      state({
        state_id: "20",
        value_set_id: "600",
        value_set_version_label: "expanded",
        delivery_column_name: "month_jan",
        operational_definition: "January expanded state",
        valid_from: "2020-01-01",
        valid_to: "2020-01-31",
        value_set_summary: coding(600, [
          { code: "0", label: "No" },
          { code: "1", label: "Yes" },
        ]),
      }),
      state({
        state_id: "20",
        value_set_id: "600",
        value_set_version_label: "expanded",
        delivery_column_name: "month_feb",
        operational_definition: "February expanded state",
        valid_from: "2020-02-01",
        valid_to: "2020-02-29",
        value_set_summary: coding(600, [
          { code: "0", label: "No" },
          { code: "1", label: "Yes" },
        ]),
      }),
    ];

    await render(ValueSetView, { fqid: FQID, states, narrowed: false });

    expect(document.querySelectorAll(".vs-list > li")).toHaveLength(1);
    await expect.element(page.getByText("month_jan")).toBeVisible();
    await expect
      .element(page.getByText("January expanded state"))
      .toBeVisible();
    await expect.element(page.getByText("month_feb")).toBeVisible();
    await expect
      .element(page.getByText("February expanded state"))
      .toBeVisible();
  });

  it("disambiguates repeated definition column labels by state window (#736)", async () => {
    const states = [
      state({
        state_id: "30",
        value_set_id: "700",
        value_set_version_label: "stable",
        delivery_column_name: "reason",
        operational_definition: "Early definition",
        valid_from: "2010-01-01",
        valid_to: "2010-12-31",
        value_set_summary: coding(700, [
          { code: "0", label: "No" },
          { code: "1", label: "Yes" },
        ]),
      }),
      state({
        state_id: "31",
        value_set_id: "700",
        value_set_version_label: "stable",
        delivery_column_name: "reason",
        operational_definition: "Later definition",
        valid_from: "2011-01-01",
        valid_to: "2011-12-31",
        value_set_summary: coding(700, [
          { code: "0", label: "No" },
          { code: "1", label: "Yes" },
        ]),
      }),
    ];

    await render(ValueSetView, { fqid: FQID, states, narrowed: false });

    await expect.element(page.getByText("reason (2010)")).toBeVisible();
    await expect.element(page.getByText("Early definition")).toBeVisible();
    await expect.element(page.getByText("reason (2011)")).toBeVisible();
    await expect.element(page.getByText("Later definition")).toBeVisible();
  });

  it("an empty coding says so; a state with NO coding stays silent", async () => {
    // The two are different facts and must not read alike: a known-empty coding
    // reports its size and explains itself, while a state that delivers free text
    // has no code surface at all.
    await render(ValueSetView, {
      fqid: FQID,
      states: [
        state({
          state_id: "40",
          value_set_id: "903",
          value_set_version_label: "Församling tom",
          variant: "doda",
          valid_from: "2020-01-01",
          valid_to: "2021-12-31",
          value_set_summary: coding(903, []),
        }),
        state({
          state_id: "41",
          value_set_id: null,
          value_set_version_label: "Fritext",
          variant: "doda",
          valid_from: "2022-01-01",
          valid_to: "2023-12-31",
        }),
        plainState,
      ],
      narrowed: false,
    });
    // The empty coding's size is on its row, not hidden behind the disclosure.
    const rows = [...document.querySelectorAll(".vs-list li")];
    const empty = rows.find((li) =>
      li.textContent?.includes("Församling tom"),
    ) as HTMLElement;
    expect(empty.querySelector(".vs-count")?.textContent).toBe("(0)");
    const free = rows.find((li) =>
      li.textContent?.includes("Fritext"),
    ) as HTMLElement;
    expect(free.querySelector(".vs-count")).toBeNull();
    expect(free.querySelector("details")).toBeNull();

    // It explains itself in place: an empty coding has nothing to open, and
    // nothing to read either — the leaf already counted it.
    expect(empty.querySelector("details")).toBeNull();
    await expect
      .element(page.getByText("This value set has no codes."))
      .toBeVisible();
    expect(vi.mocked(getValues)).not.toHaveBeenCalled();
  });

  it("shuts an open code disclosure when the states are re-resolved", async () => {
    // A `?period` Apply refetches this view WITHOUT remounting it. The open map is
    // cleared with the rest of the local view state, and the twisty is BOUND to it,
    // so the row closes with it — an expanded row over an unmounted panel would
    // read as "this coding has no codes".
    const { rerender } = await render(ValueSetView, {
      fqid: FQID,
      states: [plainState, classState],
      narrowed: false,
    });
    await page.getByText("Values (2)").click();
    await expect.element(page.getByText("Upplands Väsby")).toBeVisible();

    // A refetch yields NEW state objects, as an Apply does.
    await rerender({
      states: [{ ...plainState }, { ...classState }],
      narrowed: true,
    });
    await expect
      .element(page.getByText("Upplands Väsby"))
      .not.toBeInTheDocument();
    expect([...document.querySelectorAll("details")].some((d) => d.open)).toBe(
      false,
    );
    // Let the shut panel's last read land before the harness tears the tree down:
    // resolving onto an unmounted tree is harmless (the browser transition logs
    // nothing) but Svelte notes it as an inert-derived read in the NEXT test.
    await new Promise((resolve) => setTimeout(resolve, 50));
  });

  it("a dense integer value set renders as a range, not an expandable code dump", async () => {
    await render(ValueSetView, {
      fqid: FQID,
      states: [ageState, plainState],
      narrowed: false,
    });
    expect(normalizedText(".vs-numeric-range")).toContain(
      "Integer values 0-20 (21 values)",
    );
    await expect.element(page.getByText("Values (21)")).not.toBeInTheDocument();
  });

  it("single-state dense integer detail renders the same range summary", async () => {
    await render(ValueSetView, {
      fqid: FQID,
      states: [ageState],
      narrowed: false,
    });
    expect(normalizedText(".vs-numeric-range")).toContain(
      "Integer values 0-20 (21 values)",
    );
    await expect.element(page.getByText("0 år")).not.toBeInTheDocument();
  });

  it("single-state detail badges a pooled edition window (Y-202)", async () => {
    // A pooled-marked state shows the "pooled" badge beside its window; an
    // ordinary state shows none.
    const { rerender } = await render(ValueSetView, {
      fqid: FQID,
      states: [state({ pooled: true })],
      narrowed: false,
    });
    await expect
      .element(page.getByText("pooled", { exact: true }))
      .toBeVisible();
    await rerender({
      states: [state({ pooled: false })],
      narrowed: false,
    });
    await expect
      .element(page.getByText("pooled", { exact: true }))
      .not.toBeInTheDocument();
  });

  it("multi-state usage badges exactly the pooled rows (Y-202)", async () => {
    // Pooled/annual/pooled states of one variant and value set: the annual
    // middle must not fuse its pooled neighbours, and exactly the two pooled
    // window rows carry the badge.
    const base = {
      variant: "v",
      value_set_id: "100",
      classifications: [],
    };
    await render(ValueSetView, {
      fqid: FQID,
      states: [
        state({
          ...base,
          state_id: "1",
          period_scope: "intervals",
          valid_from: "2012-01-01",
          valid_to: "2012-12-31",
          pooled: true,
        }),
        state({
          ...base,
          state_id: "2",
          period_scope: "intervals",
          valid_from: "2013-01-01",
          valid_to: "2013-12-31",
          pooled: false,
        }),
        state({
          ...base,
          state_id: "3",
          period_scope: "intervals",
          valid_from: "2014-01-01",
          valid_to: "2014-12-31",
          pooled: true,
        }),
      ],
      narrowed: false,
    });
    expect(document.querySelectorAll(".vs-list > li")).toHaveLength(1);
    expect(document.querySelectorAll(".pooled-badge")).toHaveLength(2);
  });

  it("keeps distinct source domains even when they declare the same classification", async () => {
    // SCB ships ≥2 distinct value_set_ids per LKF edition. They are distinct source
    // domains, so each keeps its own row (each linking LKF 2007) beside the plain
    // value set; they do not collapse into one.
    const states = [
      classState, // lkf2007, value_set_id 100
      state({ ...classState, state_id: "9", value_set_id: "101" }), // SAME edition, distinct id
      plainState,
    ];
    await render(ValueSetView, { fqid: FQID, states, narrowed: false });
    await expect.element(page.getByText("Kommun historisk")).toBeVisible();
    expect(
      page.getByRole("link", { name: "LKF 2007", exact: true }).elements(),
    ).toHaveLength(2);
  });

  it("disambiguates non-classification rows that share a version label by span", async () => {
    // Two plain value sets both labelled "Kommun historisk" (kommun's ×22 case):
    // the bare label can't tell them apart, so each row appends its overall span.
    const a = state({
      state_id: "1",
      value_set_id: "10",
      value_set_version_label: "Kommun historisk",
      variant: "a",
      valid_from: "1968-01-01",
      valid_to: "1970-12-31",
    });
    const b = state({
      state_id: "2",
      value_set_id: "11",
      value_set_version_label: "Kommun historisk",
      variant: "a",
      valid_from: "1971-01-01",
      valid_to: "1973-12-31",
    });
    await render(ValueSetView, { fqid: FQID, states: [a, b], narrowed: false });
    const labels = [...document.querySelectorAll(".vs-label")].map(
      (el) => el.textContent,
    );
    expect(labels).toEqual([
      "Kommun historisk · 1968 – 1970",
      "Kommun historisk · 1971 – 1973",
    ]);
  });

  it("renders technical-change hints inside a folded value-set usage (#743)", async () => {
    const states = [
      state({
        state_id: "1",
        value_set_id: "300",
        value_set_version_label: "Kommun historisk",
        variant: "doda",
        valid_from: "2010-01-01",
        valid_to: "2010-12-31",
        data_type: "int",
        delivery_column_name: "KOMMUN",
      }),
      state({
        state_id: "2",
        value_set_id: "300",
        value_set_version_label: "Kommun historisk",
        variant: "doda",
        valid_from: "2011-01-01",
        valid_to: "2011-12-31",
        data_type: "bigint",
        delivery_column_name: "KOMMUN_ID",
      }),
    ];
    await render(ValueSetView, { fqid: FQID, states, narrowed: false });
    await expect
      .element(
        page.getByText(
          "changed 2011: type int -> bigint; column KOMMUN -> KOMMUN_ID",
        ),
      )
      .toBeVisible();
  });

  it("single-state detail omits default/noise rows and wholly unknown windows", async () => {
    await render(ValueSetView, {
      fqid: FQID,
      states: [
        state({
          variant: "_default",
          value_set_version_label: "",
          valid_from: "0001-01-01",
          valid_to: "9999-12-31",
          value_set_summary: null,
        }),
      ],
      narrowed: false,
    });
    await expect.element(page.getByText("Variant")).not.toBeInTheDocument();
    await expect
      .element(page.getByText("Value-set version"))
      .not.toBeInTheDocument();
    await expect.element(page.getByText("Valid")).not.toBeInTheDocument();
    await expect.element(page.getByText(/since 0001/)).not.toBeInTheDocument();
    await expect
      .element(page.getByText("No value set."))
      .not.toBeInTheDocument();
  });

  it("multi-state usage omits default/noise labels and wholly unknown windows", async () => {
    await render(ValueSetView, {
      fqid: FQID,
      states: [
        state({
          state_id: "10",
          variant: "_default",
          value_set_id: "700",
          value_set_version_label: "",
          valid_from: "0001-01-01",
          valid_to: "9999-12-31",
          operational_definition: "Defined from the source register.",
        }),
        state({
          state_id: "11",
          variant: "regional",
          value_set_id: "700",
          value_set_version_label: "",
          valid_from: "2010-01-01",
          valid_to: "2010-12-31",
        }),
      ],
      narrowed: false,
    });

    await expect.element(page.getByText("regional")).toBeVisible();
    await expect
      .element(page.getByText("Defined from the source register."))
      .toBeVisible();
    await expect.element(page.getByText("_default")).not.toBeInTheDocument();
    await expect
      .element(page.getByText("Unknown window"))
      .not.toBeInTheDocument();
    await expect.element(page.getByText(/since 0001/)).not.toBeInTheDocument();
  });
});
