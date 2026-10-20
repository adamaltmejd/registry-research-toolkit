import { expect, it } from "vitest";
import { page } from "vitest/browser";
import { render } from "vitest-browser-svelte";
import type { DataWarningModel } from "./api";
import DataWarnings from "./DataWarnings.svelte";

const warning: DataWarningModel = {
  warning_id: "a".repeat(64),
  register_fqid: "scb/innovation-foretag",
  variable_fqid: "scb/innovation-foretag/developer",
  code: "missing_coding_period",
  severity: "warning",
  summary: "The stored response codes are unavailable for this delivery.",
  detail: "Source question categories do not establish the stored encoding.",
  diagnostic_detail_sha256: "d".repeat(64),
  fields: [],
  refs: [],
  withheld_output: ["value_set"],
  delivery_column_name: "DEVELOPER",
  variant: "_default",
  valid_from: "2010-01-01",
  valid_to: "2012-12-31",
};

it("shows the limitation, delivery scope and unavailable metadata", async () => {
  await render(DataWarnings, { warnings: [warning] });
  await expect.element(page.getByText(warning.summary)).toBeVisible();
  await expect
    .element(page.getByText("DEVELOPER", { exact: true }))
    .toBeVisible();
  await expect
    .element(page.getByText("Unavailable metadata: response codes"))
    .toBeVisible();
  await expect
    .element(page.getByText("Warning", { exact: true }))
    .toBeVisible();
});

// Fails if the list reverts to the API's `warning_id` (hash) order: the input is
// in id order, the reader needs window order with an open start first.
it("orders warnings by window start, then end", async () => {
  const at = (id: string, from: string | null, to: string | null) => ({
    ...warning,
    warning_id: id.repeat(64),
    summary: `Window ${from ?? "open"}–${to ?? "open"}`,
    valid_from: from,
    valid_to: to,
  });
  await render(DataWarnings, {
    warnings: [
      at("a", "2015-01-01", "2016-12-31"),
      at("b", "2010-01-01", "2014-12-31"),
      at("c", "2010-01-01", "2011-12-31"),
      at("d", null, "2009-12-31"),
    ],
  });
  const summaries = page
    .getByRole("listitem")
    .elements()
    .map((item) => item.querySelector("p")?.textContent);
  expect(summaries).toEqual([
    "Window open–2009-12-31",
    "Window 2010-01-01–2011-12-31",
    "Window 2010-01-01–2014-12-31",
    "Window 2015-01-01–2016-12-31",
  ]);
});

it("does not invent a start year for a warning with only an end date", async () => {
  await render(DataWarnings, { warnings: [{ ...warning, valid_from: null }] });
  await expect.element(page.getByText("Through 2012-12-31")).toBeVisible();
  await expect.element(page.getByText(/1900/)).not.toBeInTheDocument();
});

it("omits an empty warning section", async () => {
  await render(DataWarnings, { warnings: [] });
  await expect
    .element(page.getByRole("heading", { name: "Data warnings" }))
    .not.toBeInTheDocument();
});
