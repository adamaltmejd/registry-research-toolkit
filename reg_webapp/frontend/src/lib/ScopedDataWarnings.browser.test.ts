import { beforeEach, expect, it, vi } from "vitest";
import { page } from "vitest/browser";
import { render } from "vitest-browser-svelte";
import { type DataWarningModel, getDataWarnings } from "./api";
import ScopedDataWarnings from "./ScopedDataWarnings.svelte";

vi.mock("./api", async (importOriginal) => ({
  ...(await importOriginal<typeof import("./api")>()),
  getDataWarnings: vi.fn(),
}));

const warning: DataWarningModel = {
  warning_id: "b".repeat(64),
  register_fqid: "scb/lisa",
  variable_fqid: "scb/lisa/kon",
  code: "missing_coding_period",
  severity: "warning",
  summary: "Response codes unavailable.",
  detail: "No source dictionary supplied.",
  diagnostic_detail_sha256: "d".repeat(64),
  source_subject: "original",
  fields: [],
  refs: [],
  withheld_output: ["value_set"],
};

beforeEach(() => {
  vi.mocked(getDataWarnings).mockReset();
});

it("requests the selected delivery and keeps register warnings separate", async () => {
  vi.mocked(getDataWarnings).mockResolvedValue([
    warning,
    {
      ...warning,
      warning_id: "c".repeat(64),
      variable_fqid: null,
      summary: "Unbound register evidence.",
    },
  ]);
  await render(ScopedDataWarnings, {
    fqid: "scb/lisa/kon",
    period: "2020",
    variant: "personer",
    representation: "KON",
  });
  await expect.element(page.getByText(warning.summary)).toBeVisible();
  expect(getDataWarnings).toHaveBeenCalledWith("scb/lisa/kon", {
    period: "2020",
    variant: "personer",
    representation: "KON",
    unassigned_only: false,
  });
  await expect
    .element(page.getByText("Unbound register evidence."))
    .not.toBeInTheDocument();
});

it("shows register limitations without duplicating individual variable warnings", async () => {
  vi.mocked(getDataWarnings).mockResolvedValue([
    warning,
    {
      ...warning,
      warning_id: "c".repeat(64),
      variable_fqid: null,
      summary: "Unbound register evidence.",
    },
  ]);
  await render(ScopedDataWarnings, { fqid: "scb/lisa", registerOnly: true });
  expect(getDataWarnings).toHaveBeenCalledWith("scb/lisa", {
    period: null,
    variant: null,
    representation: null,
    unassigned_only: true,
  });
  await expect
    .element(page.getByText("Unbound register evidence."))
    .toBeVisible();
  await expect.element(page.getByText(warning.summary)).not.toBeInTheDocument();
});

it("shows a failed warning request rather than implying no limitations", async () => {
  vi.mocked(getDataWarnings).mockRejectedValue(new Error("offline"));
  await render(ScopedDataWarnings, { fqid: "scb/lisa/kon" });
  await expect
    .element(page.getByRole("alert"))
    .toMatchTextContent("Could not load data warnings");
});
