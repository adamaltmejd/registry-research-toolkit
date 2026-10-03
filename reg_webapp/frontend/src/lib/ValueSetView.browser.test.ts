import { beforeEach, describe, expect, it, vi } from "vitest";
import { page } from "vitest/browser";
import { render } from "vitest-browser-svelte";
import type { ValueSetMemberModel, VariableStateModel } from "./api";
import { getValueSetCodes } from "./api";
import ValueSetView from "./ValueSetView.svelte";

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
  return { ...actual, getValueSetCodes: vi.fn() };
});

const CODES = new Map<string, ValueSetMemberModel[]>();

/** Register a coding's members with the stubbed read and return the SUMMARY the
 * state carries in their place. `stateId` registers a state's stored
 * classification-mismatch list instead of the value set's own membership. */
function coding(
  valueSetId: string | number,
  codes: ValueSetMemberModel[],
  opts: {
    stateId?: string | number;
    partition?: "canonical" | "source_extensions" | "nonstandard" | "sentinels";
    integerRange?: { min: number; max: number };
  } = {},
): VariableStateModel["value_set_summary"] {
  CODES.set(
    `${valueSetId}:${opts.stateId ?? ""}:${opts.partition ?? (opts.stateId != null ? "nonstandard" : "source_extensions")}`,
    codes,
  );
  return { code_count: codes.length, integer_range: opts.integerRange ?? null };
}

beforeEach(() => {
  vi.mocked(getValueSetCodes).mockReset();
  vi.mocked(getValueSetCodes).mockImplementation(
    async (
      valueSetId,
      {
        state = null,
        partition = "source_extensions",
        q = "",
        offset = 0,
        limit = 200,
      },
    ) => {
      const all = CODES.get(`${valueSetId}:${state ?? ""}:${partition}`) ?? [];
      const needle = q.trim().toLowerCase();
      const matched = all.filter(
        (c) =>
          c.code.toLowerCase().includes(needle) ||
          c.label.toLowerCase().includes(needle),
      );
      return {
        value_set_id: String(valueSetId),
        state_id: state,
        period_scope: "intervals",
        q,
        total: matched.length,
        offset,
        limit,
        codes: matched.slice(offset, offset + limit),
      };
    },
  );
});

// Minimal VariableStateModel — only the fields ValueSetView reads.
function state(over: Partial<VariableStateModel>): VariableStateModel {
  return {
    warning_ids: [],
    state_id: "1",
    period_scope: "intervals",
    variant: "v",
    variant_label: null,
    register_variant_id: "1",
    valid_from: "2000-01-01",
    valid_to: "2000-12-31",
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

    period_token: null,
    ...over,
  };
}

function normalizedText(selector: string): string {
  return (
    document
      .querySelector(selector)
      ?.textContent?.replace(/\s+/g, " ")
      .trim() ?? ""
  );
}

// A two-value-set fixture mirroring kommun: one classification value set (links
// out, no codes), one plain value set (expandable codes).
const classState = state({
  state_id: "1",
  value_set_id: "100",
  classifications: [
    {
      slug: "lkf2007",
      short_name: "lkf2007",
      name: "lkf2007",
      conformance: null,
    },
  ],
  value_set_version_label: "LKF",
  variant: "doda",
  valid_from: "2007-01-01",
  valid_to: "2010-12-31",
});
const plainState = state({
  state_id: "2",
  value_set_id: "200",
  classifications: [],
  value_set_version_label: "Kommun historisk",
  variant: "fodda",
  valid_from: "1961-01-01",
  valid_to: "1967-12-31",
  value_set_summary: coding(200, [
    { code: "0114", label: "Upplands Väsby" },
    { code: "0115", label: "Vallentuna" },
  ]),
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
  it("renders DISTINCT value sets, not raw states (the dedup)", async () => {
    // Four states, two value sets → two rows in the union list.
    const states = [
      classState,
      state({ ...classState, state_id: "3", valid_from: "2011-01-01" }),
      plainState,
      state({ ...plainState, state_id: "4", valid_from: "1968-01-01" }),
    ];
    await render(ValueSetView, { states, narrowed: false });
    expect(document.querySelectorAll(".vs-list > li")).toHaveLength(2);
  });

  it.each([false, true])(
    "shows literal delivery names, descriptions, definitions and units (multiple=%s)",
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
      await render(ValueSetView, { states, narrowed: false });
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

  it("shows state operational definitions when parallel columns share one value set (#736)", async () => {
    const states = [
      state({
        state_id: "10",
        value_set_id: "500",
        value_set_version_label: "vald/inte vald",
        delivery_column_name: "fedunsatreason_1",
        operational_definition: "Education was not relevant to work",
        value_set_summary: coding(500, [
          { code: "0", label: "Inte vald" },
          { code: "1", label: "Vald" },
        ]),
      }),
      state({
        state_id: "11",
        value_set_id: "500",
        value_set_version_label: "vald/inte vald",
        delivery_column_name: "fedunsatreason_2",
        operational_definition: "Education was too theoretical",
        value_set_summary: coding(500, [
          { code: "0", label: "Inte vald" },
          { code: "1", label: "Vald" },
        ]),
      }),
    ];

    await render(ValueSetView, { states, narrowed: false });

    expect(document.querySelectorAll(".vs-list > li")).toHaveLength(1);
    await expect.element(page.getByText("fedunsatreason_1")).toBeVisible();
    await expect
      .element(page.getByText("Education was not relevant to work"))
      .toBeVisible();
    await expect.element(page.getByText("fedunsatreason_2")).toBeVisible();
    await expect
      .element(page.getByText("Education was too theoretical"))
      .toBeVisible();
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

    await render(ValueSetView, { states, narrowed: false });

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

    await render(ValueSetView, { states, narrowed: false });

    await expect.element(page.getByText("reason (2010)")).toBeVisible();
    await expect.element(page.getByText("Early definition")).toBeVisible();
    await expect.element(page.getByText("reason (2011)")).toBeVisible();
    await expect.element(page.getByText("Later definition")).toBeVisible();
  });

  it("a classification value set shows the '= LKF ⟨vintage⟩' link, NOT a code dump", async () => {
    await render(ValueSetView, {
      states: [classState, plainState],
      narrowed: false,
    });
    // The classification row links out to the classification.
    const link = page.getByRole("link", { name: "LKF 2007" });
    await expect.element(link).toBeVisible();
    expect(link.element().getAttribute("href")).toBe("/catalog/class/lkf2007");
  });

  it("warns on nonconforming codes while keeping a classification link", async () => {
    coding(
      100,
      [
        { code: "01", label: "Original source meaning" },
        { code: "02", label: "Another source meaning" },
      ],
      { stateId: "1", partition: "canonical" },
    );
    await render(ValueSetView, {
      states: [
        state({
          ...classState,
          classifications: [
            {
              slug: "lkf2007",
              short_name: "lkf2007",
              name: "lkf2007",
              conformance: {
                declared_classification_slug: "lkf2007",
                declared_classification_short_name: "LKF2007",
                declared_classification_name: "Kommun historisk",
                status: "extended",
                checked_code_count: 3,
                matched_code_count: 2,
                nonconforming_code_count: 1,
                nonstandard_code_count: 1,
                sentinel_code_count: 0,
                overlap: 2 / 3,
                nonconforming_codes: [],
              },
            },
          ],
          value_set_summary: coding(100, [{ code: "X", label: "Extra code" }], {
            stateId: "1",
          }),
        }),
        plainState,
      ],
      narrowed: false,
    });
    expect(normalizedText(".conformance-notice")).toContain(
      "2 source codes match this classification; 1 is a source extension.",
    );
    const summary = page.getByText("Nonstandard source codes (1)");
    await summary.click();
    await expect.element(page.getByText("Extra code")).toBeVisible();
    await page.getByText("Matching source codes (2)").click();
    await expect
      .element(page.getByText("Original source meaning"))
      .toBeVisible();
    expect(
      vi
        .mocked(getValueSetCodes)
        .mock.calls.some(
          ([, options]) =>
            options.partition === "canonical" && options.state === "1",
        ),
    ).toBe(true);
  });

  it("keeps EVERY warning state's mismatch list reachable after the collapse", async () => {
    // One classification edition, two codings, a DIFFERENT stored list on each.
    // The row collapses them (M13) — the lists must not collapse with them, and
    // each disclosure's count must describe the list it opens.
    const conformance = {
      declared_classification_slug: "lkf2007",
      declared_classification_short_name: "LKF2007",
      declared_classification_name: "Kommun historisk",
      status: "extended" as const,
      checked_code_count: 3,
      matched_code_count: 2,
      nonconforming_code_count: 1,
      nonstandard_code_count: 1,
      sentinel_code_count: 0,
      overlap: 2 / 3,
      nonconforming_codes: [],
    };
    await render(ValueSetView, {
      states: [
        state({
          ...classState,
          state_id: "11",
          period_scope: "intervals",
          value_set_id: "101",
          value_set_version_label: "LKF 2007 rev A",
          variant: "fodda",
          valid_from: "1981-01-01",
          valid_to: "1981-12-31",
          classifications: [
            {
              slug: conformance.declared_classification_slug,
              short_name: conformance.declared_classification_slug,
              name: conformance.declared_classification_slug,
              conformance: conformance,
            },
          ],
          value_set_summary: coding(101, [{ code: "X", label: "Extra" }], {
            stateId: "11",
          }),
        }),
        state({
          ...classState,
          state_id: "12",
          period_scope: "intervals",
          value_set_id: "102",
          value_set_version_label: "LKF 2007 rev B",
          variant: "flytt",
          valid_from: "1982-01-01",
          valid_to: "1982-12-31",
          classifications: [
            {
              slug: {
                ...conformance,
                checked_code_count: 4,
                matched_code_count: 2,
                nonconforming_code_count: 2,
                nonstandard_code_count: 2,
                sentinel_code_count: 0,
              }.declared_classification_slug,
              short_name: {
                ...conformance,
                checked_code_count: 4,
                matched_code_count: 2,
                nonconforming_code_count: 2,
                nonstandard_code_count: 2,
                sentinel_code_count: 0,
              }.declared_classification_slug,
              name: {
                ...conformance,
                checked_code_count: 4,
                matched_code_count: 2,
                nonconforming_code_count: 2,
                nonstandard_code_count: 2,
                sentinel_code_count: 0,
              }.declared_classification_slug,
              conformance: {
                ...conformance,
                checked_code_count: 4,
                matched_code_count: 2,
                nonconforming_code_count: 2,
                nonstandard_code_count: 2,
                sentinel_code_count: 0,
              },
            },
          ],
          value_set_summary: coding(
            102,
            [
              { code: "Y", label: "Later extra" },
              { code: "Z", label: "Later still" },
            ],
            { stateId: "12" },
          ),
        }),
        plainState,
      ],
      narrowed: false,
    });
    // ONE row for the edition, but one notice per stored verdict — each labelled
    // with the period/variant it was stored for.
    const notices = [...document.querySelectorAll(".conformance-notice")];
    expect(notices).toHaveLength(2);
    expect(
      notices.map((n) =>
        n
          .querySelector(".conformance-scope")
          ?.textContent?.replace(/\s+/g, " ")
          .trim(),
      ),
    ).toEqual([
      "LKF 2007 rev A recorded for 1981 fodda",
      "LKF 2007 rev B recorded for 1982 flytt",
    ]);

    // Both lists open, each from its own state — X here, Y and Z there.
    await page.getByText("Nonstandard source codes (1)").click();
    await expect
      .element(page.getByText("Extra", { exact: true }))
      .toBeVisible();
    await page.getByText("Nonstandard source codes (2)").click();
    await expect.element(page.getByText("Later extra")).toBeVisible();
    await expect.element(page.getByText("Later still")).toBeVisible();
    // Each list is read by ITS OWN state — no disclosure reads another's coding.
    const read = new Set(
      vi
        .mocked(getValueSetCodes)
        .mock.calls.map(([id, opts]) => `${id}:${opts.state}`),
    );
    expect([...read].sort()).toEqual(["101:11", "102:12"]);
  });

  it("an empty coding says so; a state with NO coding stays silent", async () => {
    // The two are different facts and must not read alike: a known-empty coding
    // reports its size and explains itself, while a state that delivers free text
    // has no code surface at all.
    await render(ValueSetView, {
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
    expect(vi.mocked(getValueSetCodes)).not.toHaveBeenCalled();
  });

  it("shows the claimed classification alongside source extensions", async () => {
    await render(ValueSetView, {
      states: [
        state({
          value_set_id: "300",
          classifications: [
            {
              slug: "isced-f2013",
              short_name: "isced-f2013",
              name: "isced-f2013",
              conformance: {
                declared_classification_slug: "isced-f2013",
                declared_classification_short_name: "ISCED-F 2013",
                declared_classification_name: "ISCED-F 2013",
                status: "extended",
                checked_code_count: 25,
                matched_code_count: 1,
                nonconforming_code_count: 24,
                nonstandard_code_count: 24,
                sentinel_code_count: 0,
                overlap: 0.04,
                nonconforming_codes: [],
              },
            },
          ],
          value_set_version_label: "ISCED F 2013",
          value_set_summary: coding(300, [
            { code: "13", label: "Datavetenskap" },
            { code: "1a", label: "Pedagogik" },
          ]),
        }),
        plainState,
      ],
      narrowed: false,
    });
    expect(normalizedText(".conformance-notice")).toContain(
      "1 source code matches this classification; 24 are source extensions.",
    );
    await expect
      .element(page.getByRole("link", { name: "isced-f2013" }).first())
      .toBeVisible();
    await expect
      .element(page.getByText("Matching source codes (1)"))
      .toBeVisible();
    await expect
      .element(page.getByText("Nonstandard source codes (24)"))
      .toBeVisible();
  });

  it("a plain value set exposes its codes inline (expandable), not the classification link", async () => {
    // ≥2 states → the multi-state union (one state alone is single-state DETAIL).
    await render(ValueSetView, {
      states: [plainState, classState],
      narrowed: false,
    });
    // The "Values (2)" disclosure is present (the plain value set); expanding
    // reveals the code rows.
    const summary = page.getByText("Values (2)");
    await expect.element(summary).toBeVisible();
    await summary.click();
    await expect.element(page.getByText("Upplands Väsby")).toBeVisible();
  });

  it("shuts an open code disclosure when the states are re-resolved", async () => {
    // A `?period` Apply refetches this view WITHOUT remounting it. The open map is
    // cleared with the rest of the local view state, and the twisty is BOUND to it,
    // so the row closes with it — an expanded row over an unmounted panel would
    // read as "this coding has no codes".
    const { rerender } = await render(ValueSetView, {
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
      states: [ageState, plainState],
      narrowed: false,
    });
    expect(normalizedText(".vs-numeric-range")).toContain(
      "Integer values 0-20 (21 values)",
    );
    await expect.element(page.getByText("Values (21)")).not.toBeInTheDocument();
  });

  it("single-state dense integer detail renders the same range summary", async () => {
    await render(ValueSetView, { states: [ageState], narrowed: false });
    expect(normalizedText(".vs-numeric-range")).toContain(
      "Integer values 0-20 (21 values)",
    );
    await expect.element(page.getByText("0 år")).not.toBeInTheDocument();
  });

  it("single-state detail badges a pooled edition window (Y-202)", async () => {
    // A pooled-marked state shows the "pooled" badge beside its window; an
    // ordinary state shows none.
    const { rerender } = await render(ValueSetView, {
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
    // The duplicate-LKF-row bug: SCB ships ≥2 distinct value_set_ids per LKF
    // edition. Two such states for lkf2007 must render ONE "= LKF 2007" row, not
    // two — plus the one plain value set → two rows total.
    const states = [
      classState, // lkf2007, value_set_id 100
      state({ ...classState, state_id: "9", value_set_id: "101" }), // SAME edition, distinct id
      plainState,
    ];
    await render(ValueSetView, { states, narrowed: false });
    expect(document.querySelectorAll(".vs-list > li")).toHaveLength(3);
    // Exactly one "= LKF 2007" link (no duplicate row).
    expect(
      document.querySelectorAll('a[href="/catalog/class/lkf2007"]'),
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
    await render(ValueSetView, { states: [a, b], narrowed: false });
    const labels = [...document.querySelectorAll(".vs-label")].map(
      (el) => el.textContent,
    );
    expect(labels).toEqual([
      "Kommun historisk · 1968 – 1970",
      "Kommun historisk · 1971 – 1973",
    ]);
  });

  it("per-row Isolate focuses one value set; '← All value sets' returns to the union", async () => {
    await render(ValueSetView, {
      states: [classState, plainState],
      narrowed: false,
    });
    // Union by default: two list rows, each with an Isolate button.
    expect(document.querySelectorAll(".vs-list > li")).toHaveLength(2);
    const isolateButtons = page.getByRole("button", { name: "Isolate" });
    // Isolate the FIRST row (the classification one).
    await isolateButtons.first().click();
    expect(document.querySelectorAll(".vs-list > li")).toHaveLength(0);
    await expect.element(page.getByText("Used by")).toBeVisible();
    // The reset returns to the union.
    await page.getByRole("button", { name: "← All value sets" }).click();
    expect(document.querySelectorAll(".vs-list > li")).toHaveLength(2);
  });

  it("the FilterInput narrows the union list (by label / variant slug)", async () => {
    await render(ValueSetView, {
      states: [classState, plainState],
      narrowed: false,
    });
    expect(document.querySelectorAll(".vs-list > li")).toHaveLength(2);
    // Filter to the plain value set by its label substring.
    const filter = page.getByRole("textbox", { name: "Filter value sets" });
    await filter.fill("historisk");
    expect(document.querySelectorAll(".vs-list > li")).toHaveLength(1);
    await expect
      .element(page.getByText("Kommun historisk", { exact: true }))
      .toBeVisible();
  });

  it("Isolate after filtering isolates the FILTERED value set (stable key, not list index)", async () => {
    // `plainState` is the SECOND value set in the unfiltered list. Filtering to it
    // leaves a single row whose Isolate must focus IT — not the first of the
    // unfiltered list. If isolation keyed on a list INDEX, the filtered row's
    // index 0 would wrongly isolate `classState` (= LKF 2007); keying on the
    // stable `vs.key` isolates the right one.
    await render(ValueSetView, {
      states: [classState, plainState],
      narrowed: false,
    });
    const filter = page.getByRole("textbox", { name: "Filter value sets" });
    await filter.fill("historisk");
    expect(document.querySelectorAll(".vs-list > li")).toHaveLength(1);
    await page.getByRole("button", { name: "Isolate" }).click();
    // The isolated detail shows the plain value set, NOT the classification one:
    // its heading reads "Kommun historisk" and there is no LKF classification link.
    await expect.element(page.getByText("Used by")).toBeVisible();
    expect(
      document.querySelector(".vs-detail .vs-heading")?.textContent,
    ).toContain("Kommun historisk");
    expect(
      document.querySelector('a[href="/catalog/class/lkf2007"]'),
    ).toBeNull();
  });

  it("a plain value set with no inline value_set omits filler text when isolated", async () => {
    // A plain (non-classification) value set whose `value_set` is null/empty: the
    // isolated body has no codes to dump and no classification to link, so it
    // renders no filler text.
    const codeless = state({
      state_id: "1",
      value_set_id: "300",
      value_set_version_label: "Codeless",
      variant: "a",
      value_set_summary: null,
    });
    const other = state({
      state_id: "2",
      value_set_id: "301",
      value_set_version_label: "Other",
      variant: "a",
    });
    await render(ValueSetView, {
      states: [codeless, other],
      narrowed: false,
    });
    await page.getByRole("button", { name: "Isolate" }).first().click();
    await expect.element(page.getByText("Codeless")).toBeVisible();
    await expect
      .element(page.getByText("No value set."))
      .not.toBeInTheDocument();
  });

  it("does NOT render any resolution-narrowing picker (the picker owns that now)", async () => {
    // #905: the old variant / value-set-version chips moved to RepresentationPicker.
    // The viewer is pure display — no `.picker` fieldset, regardless of variant
    // multiplicity.
    await render(ValueSetView, {
      states: [classState, plainState], // distinct variants doda / fodda
      narrowed: false,
    });
    expect(document.querySelector(".picker")).toBeNull();
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
    await render(ValueSetView, { states, narrowed: false });
    await expect
      .element(
        page.getByText(
          "changed 2011: type int -> bigint; column KOMMUN -> KOMMUN_ID",
        ),
      )
      .toBeVisible();
  });

  it("collapses period-out-of-scope value sets behind a disclosure (#744)", async () => {
    const inScopePlain = state({
      state_id: "3",
      value_set_id: "201",
      value_set_version_label: "In-period plain",
      variant: "doda",
      valid_from: "2008-01-01",
      valid_to: "2008-12-31",
    });
    await render(ValueSetView, {
      states: [classState, inScopePlain, plainState],
      scopeStates: [classState, inScopePlain],
      narrowed: true,
    });
    const inlineRows = document.querySelectorAll(
      "ul.vs-list:not(.out-of-period-list) > li",
    );
    expect(inlineRows).toHaveLength(2);
    expect(inlineRows[0].textContent).toContain("LKF 2007");
    expect(
      [...inlineRows].map((row) => row.textContent).join(" "),
    ).not.toContain("Kommun historisk");

    const disclosure = page.getByText("1 value set outside this period");
    await expect.element(disclosure).toBeVisible();
    await expect
      .element(page.getByText("Kommun historisk", { exact: true }))
      .not.toBeVisible();
    await disclosure.click();
    await expect
      .element(page.getByText("Kommun historisk", { exact: true }))
      .toBeVisible();
  });

  it("counts filtered matches inside the outside-period disclosure (#744 review)", async () => {
    const inScopePlain = state({
      state_id: "3",
      value_set_id: "201",
      value_set_version_label: "In-period plain",
      variant: "doda",
      valid_from: "2008-01-01",
      valid_to: "2008-12-31",
    });
    await render(ValueSetView, {
      states: [classState, inScopePlain, plainState],
      scopeStates: [classState, inScopePlain],
      narrowed: true,
    });
    const filter = page.getByRole("textbox", { name: "Filter value sets" });
    await filter.fill("historisk");
    await expect.element(page.getByText("1 of 3")).toBeVisible();
    await expect
      .element(page.getByText("1 value set outside this period"))
      .toBeVisible();
  });

  it("single-state DETAIL mode is unchanged (Variant / Valid / value set)", async () => {
    await render(ValueSetView, {
      states: [
        state({
          variant: "doda",
          value_set_version_label: "Kommun historisk",
          value_set_id: "900",
          value_set_summary: coding(900, [
            { code: "0114", label: "Upplands Väsby" },
          ]),
        }),
      ],
      narrowed: false,
    });
    // The single-state detail renders its own dl.meta + the value-set heading —
    // NOT the multi-state value-set list UI (`.vs-list`, which only the >1-state
    // view emits), so this really guards the single/multi boundary.
    await expect.element(page.getByText("Variant")).toBeVisible();
    await expect.element(page.getByText("Value-set version")).toBeVisible();
    expect(document.querySelector(".vs-list")).toBeNull();
    await expect.element(page.getByText("Upplands Väsby")).toBeVisible();
  });

  it("single-state detail omits default/noise rows and wholly unknown windows", async () => {
    await render(ValueSetView, {
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

  it("empty mode is unchanged (clean no-state message, not an error)", async () => {
    await render(ValueSetView, { states: [], narrowed: true });
    await expect
      .element(page.getByText("No state delivered for this period."))
      .toBeVisible();
  });

  it("single-state detail is PERIOD-AWARE: a 1-state variable viewed OUTSIDE its window shows the no-state message, NOT the detail (Fix C)", async () => {
    // #905, Codex P2: a variable with exactly ONE historical state, viewed at a
    // `?period` OUTSIDE that state. The leaf passes the full history (1 state) but an
    // EMPTY period scope. `single` must key off the SCOPE (zero in-period → no single
    // detail) and fall through to the "No state delivered" path — not render the lone
    // state's detail as if it were in-period.
    const lone = state({
      variant: "doda",
      value_set_version_label: "Kommun historisk",
      value_set_id: "900",
      value_set_summary: coding(900, [
        { code: "0114", label: "Upplands Väsby" },
      ]),
      valid_from: "2007-01-01",
      valid_to: "2010-12-31",
    });
    await render(ValueSetView, {
      states: [lone],
      scopeStates: [], // the period delivered ZERO of this variable's states
      narrowed: true,
    });
    // Full history (1 state) is present, so this lands in the multi-state branch's
    // empty-period hint (NOT the bare empty branch) — but it still tells the user no
    // state was delivered for the period, and the historical state is collapsed.
    await expect
      .element(
        page.getByText(/No state delivered for this period\./, {
          exact: false,
        }),
      )
      .toBeVisible();
    // The single-state DETAIL block must NOT render (the lone state is collapsed as a
    // historical value set, not surfaced as the in-period detail).
    expect(document.querySelector(".state-detail")).toBeNull();
  });

  it("single-state detail is PERIOD-AWARE: a 1-state variable viewed IN its window still shows the detail (Fix C)", async () => {
    // The control: the SAME lone state, with a period scope that DID deliver it →
    // exactly one in-period state → the single-state detail renders.
    const lone = state({
      variant: "doda",
      value_set_version_label: "Kommun historisk",
      value_set_id: "900",
      value_set_summary: coding(900, [
        { code: "0114", label: "Upplands Väsby" },
      ]),
      valid_from: "2007-01-01",
      valid_to: "2010-12-31",
    });
    await render(ValueSetView, {
      states: [lone],
      scopeStates: [lone],
      narrowed: true,
    });
    await expect.element(page.getByText("Variant")).toBeVisible();
    expect(document.querySelector(".vs-list")).toBeNull();
    await expect.element(page.getByText("Upplands Väsby")).toBeVisible();
  });

  it("a filter that hides the in-period rows does NOT mis-report 'No state delivered for this period' (Codex P3)", async () => {
    // The period DID deliver in-period value sets (classState + inScopePlain), but a
    // text filter matches only the OUT-of-period row's variant ("fodda"). The empty
    // hint must key off the UNFILTERED period scope (which is non-empty), so it must
    // NOT appear — the union branch's own "no matches" describes the filtered-out
    // state instead.
    const inScopePlain = state({
      state_id: "3",
      value_set_id: "201",
      value_set_version_label: "In-period plain",
      variant: "doda",
      valid_from: "2008-01-01",
      valid_to: "2008-12-31",
    });
    await render(ValueSetView, {
      states: [classState, inScopePlain, plainState],
      scopeStates: [classState, inScopePlain],
      narrowed: true,
    });
    const filter = page.getByRole("textbox", { name: "Filter value sets" });
    // "fodda" is only the out-of-period plainState's variant → in-period shown rows
    // become empty, but the out-of-period row still matches and stays collapsed.
    await filter.fill("fodda");
    // The out-of-period row stays collapsed (the filter matched it), which renders the
    // union branch — proving we did NOT fall into the "No state delivered" branch.
    await expect
      .element(page.getByText("1 value set outside this period"))
      .toBeVisible();
    // The mis-report must be entirely absent from the DOM (the in-period scope is
    // non-empty), not merely hidden.
    expect(document.body.textContent).not.toContain(
      "No state delivered for this period.",
    );
  });

  // ── focusColumn deep-link (#905) ────────────────────────────────────────────
  it("focusColumn auto-isolates the distinct value set its column delivers", async () => {
    // `plainState` is delivered via column PLAINCOL; the `?codes=PLAINCOL` deep link
    // (focusColumn) seeds the isolation onto its value set, NOT the classification
    // one — the union list is hidden and the isolated detail shows it.
    const classCol = state({ ...classState, delivery_column_name: "CLASSCOL" });
    const plainCol = state({ ...plainState, delivery_column_name: "PLAINCOL" });
    await render(ValueSetView, {
      states: [classCol, plainCol],
      narrowed: false,
      focusColumn: "PLAINCOL",
    });
    // Isolated → no union rows, the detail's "Used by" + the plain value set's
    // heading are visible.
    expect(document.querySelectorAll(".vs-list > li")).toHaveLength(0);
    await expect.element(page.getByText("Used by")).toBeVisible();
    expect(
      document.querySelector(".vs-detail .vs-heading")?.textContent,
    ).toContain("Kommun historisk");
  });

  it("focusColumn on a coding-VARYING column isolates the LATEST-era value set", async () => {
    // One column delivered two distinct value sets over time (a coding change):
    // the deep link isolates the LATEST-era one (max valid_to) — the picker row's
    // representative coding. The earlier coding stays one "← All value sets" away.
    const early = state({
      state_id: "1",
      value_set_id: "303",
      value_set_version_label: "Old coding",
      variant: "v",
      delivery_column_name: "COL",
      valid_from: "2015-01-01",
      valid_to: "2018-12-31",
    });
    const latest = state({
      state_id: "2",
      value_set_id: "249",
      value_set_version_label: "New coding",
      variant: "v",
      delivery_column_name: "COL",
      valid_from: "2019-01-01",
      valid_to: "2022-12-31",
    });
    await render(ValueSetView, {
      states: [early, latest],
      narrowed: false,
      focusColumn: "COL",
    });
    await expect.element(page.getByText("Used by")).toBeVisible();
    expect(
      document.querySelector(".vs-detail .vs-heading")?.textContent,
    ).toContain("New coding");
    // The reset returns to the union showing BOTH codings.
    await page.getByRole("button", { name: "← All value sets" }).click();
    expect(document.querySelectorAll(".vs-list > li")).toHaveLength(2);
  });

  it("re-seeds the isolation when the states change underneath (sibling navigation)", async () => {
    // The reset `$effect` (keyed on `states`) must re-run when navigation swaps the
    // states for a sibling column: a stale isolated detail can't survive into the new
    // view. Render with COL → "First coding" focused, then rerender with a DIFFERENT
    // `states` set where COL now delivers "Second coding"; the isolation must FOLLOW
    // to the new value set, not strand the old one.
    const first = state({
      state_id: "1",
      value_set_id: "401",
      value_set_version_label: "First coding",
      variant: "v",
      delivery_column_name: "COL",
      valid_from: "2010-01-01",
      valid_to: "2012-12-31",
    });
    const firstOther = state({
      state_id: "2",
      value_set_id: "402",
      value_set_version_label: "First other",
      variant: "v",
      delivery_column_name: "OTHER",
      valid_from: "2010-01-01",
      valid_to: "2012-12-31",
    });
    const { rerender } = await render(ValueSetView, {
      states: [first, firstOther],
      narrowed: false,
      focusColumn: "COL",
    });
    expect(
      document.querySelector(".vs-detail .vs-heading")?.textContent,
    ).toContain("First coding");

    // Navigate to a sibling: a NEW states set where COL delivers a different coding.
    const second = state({
      state_id: "3",
      value_set_id: "501",
      value_set_version_label: "Second coding",
      variant: "v",
      delivery_column_name: "COL",
      valid_from: "2013-01-01",
      valid_to: "2015-12-31",
    });
    const secondOther = state({
      state_id: "4",
      value_set_id: "502",
      value_set_version_label: "Second other",
      variant: "v",
      delivery_column_name: "OTHER",
      valid_from: "2013-01-01",
      valid_to: "2015-12-31",
    });
    await rerender({
      states: [second, secondOther],
      narrowed: false,
      focusColumn: "COL",
    });
    // The isolation re-seeded onto the NEW value set; the stale one is gone.
    const heading = document.querySelector(
      ".vs-detail .vs-heading",
    )?.textContent;
    expect(heading).toContain("Second coding");
    expect(heading).not.toContain("First coding");
  });

  it("isolates a focusColumn value set even when it is OUT of period (isolate beats period-collapse)", async () => {
    // A `?period` collapses out-of-period value sets behind a disclosure (#744), but
    // a `?codes=<column>` deep link must still land on its target even when that
    // column's value set falls OUTSIDE the period. The isolate path (keyed on the
    // value set regardless of period) takes precedence over the period-collapse: the
    // focused detail renders fully, not buried under "… outside this period".
    const inPeriodCol = state({
      state_id: "1",
      value_set_id: "600",
      value_set_version_label: "In-period coding",
      variant: "v",
      delivery_column_name: "INCOL",
      valid_from: "2008-01-01",
      valid_to: "2008-12-31",
    });
    const outOfPeriodCol = state({
      state_id: "2",
      value_set_id: "601",
      value_set_version_label: "Out-of-period coding",
      variant: "v",
      delivery_column_name: "OUTCOL",
      valid_from: "1990-01-01",
      valid_to: "1995-12-31",
    });
    await render(ValueSetView, {
      states: [inPeriodCol, outOfPeriodCol],
      // scopeStates covers ONLY the in-period value set.
      scopeStates: [inPeriodCol],
      narrowed: true,
      // …but the deep link focuses the OUT-of-period column.
      focusColumn: "OUTCOL",
    });
    // The isolated detail is fully visible (not collapsed): its heading shows the
    // out-of-period coding and "Used by" is present, with no period disclosure in
    // the way.
    await expect.element(page.getByText("Used by")).toBeVisible();
    expect(
      document.querySelector(".vs-detail .vs-heading")?.textContent,
    ).toContain("Out-of-period coding");
    // The period-collapse disclosure does not gate the focused detail.
    expect(document.querySelector(".out-of-period")).toBeNull();
  });

  it("focusColumn degrades to the default union when no state delivers it", async () => {
    // A stale / unknown `?codes=` matches nothing → the viewer shows its default
    // union list, not a blank isolated detail.
    const classCol = state({ ...classState, delivery_column_name: "CLASSCOL" });
    const plainCol = state({ ...plainState, delivery_column_name: "PLAINCOL" });
    await render(ValueSetView, {
      states: [classCol, plainCol],
      narrowed: false,
      focusColumn: "NOPE",
    });
    expect(document.querySelectorAll(".vs-list > li")).toHaveLength(2);
    expect(document.querySelector(".vs-detail")).toBeNull();
  });

  it("focusVariant isolates the clicked variant's coding when a column is shared across variants (#905)", async () => {
    // One delivery column COL delivered by TWO variants with DISTINCT codings —
    // picker rows are keyed `(variant, column)`, so the deep link carries the variant.
    // `focusVariant: "a"` must isolate variant a's coding, NOT variant b's latest-era
    // one (the unscoped column lookup would pick b).
    const a = state({
      state_id: "1",
      value_set_id: "100",
      value_set_version_label: "Coding A",
      variant: "a",
      delivery_column_name: "COL",
      valid_from: "2015-01-01",
      valid_to: "2018-12-31",
    });
    const b = state({
      state_id: "2",
      value_set_id: "200",
      value_set_version_label: "Coding B",
      variant: "b",
      delivery_column_name: "COL",
      valid_from: "2019-01-01",
      valid_to: "2022-12-31",
    });
    const { rerender } = await render(ValueSetView, {
      states: [a, b],
      narrowed: false,
      focusColumn: "COL",
      focusVariant: "a",
    });
    await expect.element(page.getByText("Used by")).toBeVisible();
    const headingA = document.querySelector(
      ".vs-detail .vs-heading",
    )?.textContent;
    expect(headingA).toContain("Coding A");
    expect(headingA).not.toContain("Coding B");
    // Re-render with the OTHER variant: same column, the other row's coding. The
    // reset $effect re-seeds the isolation onto b's value set.
    await rerender({
      states: [a, b],
      narrowed: false,
      focusColumn: "COL",
      focusVariant: "b",
    });
    const headingB = document.querySelector(
      ".vs-detail .vs-heading",
    )?.textContent;
    expect(headingB).toContain("Coding B");
    expect(headingB).not.toContain("Coding A");
  });
});

it("keeps every declared book and reads scoped special codes using exact large IDs", async () => {
  const stateId = "9007199254740993";
  const valueSetId = "9007199254740992";
  const verdict = {
    declared_classification_slug: "sni2007",
    declared_classification_short_name: "SNI 2007",
    declared_classification_name: "SNI 2007",
    status: "extended" as const,
    checked_code_count: 2,
    matched_code_count: 1,
    nonconforming_code_count: 1,
    nonstandard_code_count: 0,
    sentinel_code_count: 1,
    overlap: 0.5,
    nonconforming_codes: [],
  };
  vi.mocked(getValueSetCodes).mockImplementation(async (id, options) => ({
    value_set_id: id,
    state_id: options.state ?? null,
    q: "",
    total: 1,
    offset: 0,
    limit: 200,
    codes:
      options.state == null
        ? [{ code: "0", label: "Original source label" }]
        : [
            {
              code: "0",
              label: "Original source label",
              member_kind: "sentinel",
              sentinel_meaning: null,
              scoped_sentinels: [
                {
                  valid_from: "2013-01-01",
                  valid_to: "2013-12-31",
                  delivery_column_name: "NgS1",
                  classification_sha256: "a".repeat(64),
                  source_fingerprints: ["b".repeat(64)],
                  members: [["0", "No recorded industry"]],
                  provenance:
                    "Reviewed source coding for this column and period.",
                },
              ],
            },
          ],
  }));
  await render(ValueSetView, {
    states: [
      state({
        state_id: stateId,
        value_set_id: valueSetId,
        value_set_summary: { code_count: 2, integer_range: null },
        coding_window_from: "2013-01-01",
        delivery_column_name: "NgS1",
        classifications: [
          {
            slug: "sni2007",
            short_name: "SNI 2007",
            name: "SNI 2007",
            conformance: verdict,
          },
          {
            slug: "sni2002",
            short_name: "SNI 2002",
            name: "SNI 2002",
            conformance: null,
          },
        ],
      }),
    ],
    narrowed: false,
  });
  expect(
    document.querySelector('a[href="/catalog/class/sni2007"]'),
  ).not.toBeNull();
  expect(
    document.querySelector('a[href="/catalog/class/sni2002"]'),
  ).not.toBeNull();
  await page.getByText("Special source codes (1)").click();
  await expect
    .element(page.getByText("Original source label").first())
    .toBeVisible();
  await expect
    .element(page.getByText("No recorded industry", { exact: false }))
    .toBeVisible();
  const request = vi.mocked(getValueSetCodes).mock.lastCall;
  expect(request?.[0]).toBe(valueSetId);
  expect(request?.[1]).toMatchObject({
    state: stateId,
    classification: "sni2007",
    partition: "sentinels",
    column: "NgS1",
    alias_window_from: "2013-01-01",
  });
  await page.getByRole("button", { name: "Source evidence" }).click();
  await expect
    .element(
      page.getByText("Reviewed source coding for this column and period."),
    )
    .toBeVisible();
});

it("limits conformance notices to the selected delivery while preserving historical usage", async () => {
  const conformance = {
    declared_classification_slug: "sni2007",
    declared_classification_short_name: "SNI 2007",
    declared_classification_name: "SNI 2007",
    status: "extended" as const,
    checked_code_count: 2,
    matched_code_count: 1,
    nonconforming_code_count: 1,
    nonstandard_code_count: 1,
    sentinel_code_count: 0,
    overlap: 0.5,
    nonconforming_codes: [],
  };
  const historical = state({
    state_id: "10",
    variant: "v",
    value_set_id: "100",
    valid_from: "1980-01-01",
    valid_to: "1991-12-31",
    classifications: [
      {
        slug: "sni2007",
        short_name: "SNI 2007",
        name: "SNI 2007",
        conformance,
      },
    ],
  });
  const selected = state({
    ...historical,
    state_id: "11",
    valid_from: "1992-01-01",
    valid_to: "1992-12-31",
  });
  await render(ValueSetView, {
    states: [historical, selected],
    scopeStates: [selected],
    narrowed: true,
  });
  expect(document.querySelectorAll(".conformance-notice")).toHaveLength(1);
  expect(normalizedText(".conformance-scope")).toContain("recorded for 1992");
  expect(normalizedText(".conformance-scope")).not.toContain("1980");
  expect(normalizedText(".vs-usage")).toContain("1980 – 1992");
});
