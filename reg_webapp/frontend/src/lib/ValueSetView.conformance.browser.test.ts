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

// Split from ValueSetView.browser.test.ts by contract surface:
// classification links and conformance notices.
// Siblings: ValueSetView.browser.test.ts, ValueSetView.{conformance,isolate,period}.browser.test.ts.

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

describe("ValueSetView — value-set-centric multi-state view (#668/#905)", () => {
  it("a classification value set shows the '= LKF ⟨vintage⟩' link, NOT a code dump", async () => {
    // The classification state carries codes too, so a code dump would show as a
    // second "Values (…)" disclosure beside the plain value set's.
    await render(ValueSetView, {
      fqid: FQID,
      states: [
        state({
          ...classState,
          value_set_summary: coding(100, [{ code: "01", label: "Kod ett" }]),
        }),
        plainState,
      ],
      narrowed: false,
    });
    // The classification row links out to the classification.
    const link = page.getByRole("link", { name: "LKF 2007" });
    await expect.element(link).toBeVisible();
    expect(link.element().getAttribute("href")).toBe("/catalog/class/lkf2007");
    expect(page.getByText(/^Values \(/).elements()).toHaveLength(1);
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
      fqid: FQID,
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
    expect(
      page
        .getByRole("link")
        .elements()
        .some((a) => a.getAttribute("href") === "/catalog/class/lkf2007"),
    ).toBe(true);
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
        .mocked(getValues)
        .mock.calls.some(
          ([, options]) =>
            options?.partition === "canonical" && options?.state === "1",
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
      fqid: FQID,
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
      vi.mocked(getValues).mock.calls.map(([, opts]) => opts?.state),
    );
    expect([...read].sort()).toEqual(["11", "12"]);
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
  vi.mocked(getValues).mockImplementation(async (_ref, options) => ({
    next_cursor: null,
    total: 1,
    items:
      options?.classification == null
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
    fqid: FQID,
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
  const request = vi.mocked(getValues).mock.lastCall;
  expect(request?.[0]).toBe(FQID);
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
    fqid: FQID,
    states: [historical, selected],
    scopeStates: [selected],
    narrowed: true,
  });
  expect(document.querySelectorAll(".conformance-notice")).toHaveLength(1);
  expect(normalizedText(".conformance-scope")).toContain("recorded for 1992");
  expect(normalizedText(".conformance-scope")).not.toContain("1980");
  expect(normalizedText(".vs-usage")).toContain("1980 – 1992");
});
