import { describe, expect, it, vi } from "vitest";
import { page } from "vitest/browser";
import { render } from "vitest-browser-svelte";
import PeriodEditor from "./PeriodEditor.svelte";
import type { Period } from "./project_data";

// Scenario 3 (#201): the three real bugs this covers all reached merge once —
// a period defaulting to free text, a cross-source radio-group collision, and
// stale state after a middle-source remove. PeriodEditor is self-contained
// (props in, `onchange` out), so these drive it directly.
describe("PeriodEditor", () => {
  it("switches modes and emits the right Period shape per mode", async () => {
    const onchange = vi.fn<(next: Period) => void>();
    // A fresh source seeds `period: ""` → YEARS mode by default (year-range-first).
    const screen = await render(PeriodEditor, {
      period: "",
      issues: [],
      onchange,
    });

    await expect
      .element(screen.getByRole("radio", { name: "Years" }))
      .toBeChecked();
    await expect
      .element(screen.getByRole("spinbutton", { name: "From" }))
      .toBeVisible();

    // → Token: emits the trimmed text (empty here), swaps in the token field.
    await screen.getByRole("radio", { name: "Token" }).click();
    await expect.element(screen.getByRole("textbox")).toBeVisible();
    expect(onchange).toHaveBeenLastCalledWith("");

    // → Default: emits the "_default" sentinel.
    await screen.getByRole("radio", { name: "Default" }).click();
    await expect.element(screen.getByText(/snapshot sentinel/)).toBeVisible();
    expect(onchange).toHaveBeenLastCalledWith("_default");

    // → Years: blank spinners emit a {from,to} of raw strings (backend flags them).
    await screen.getByRole("radio", { name: "Years" }).click();
    await expect
      .element(screen.getByRole("spinbutton", { name: "To" }))
      .toBeVisible();
    expect(onchange).toHaveBeenLastCalledWith({ from: "", to: "" });
  });

  it("emits a bare year int when From === To", async () => {
    const onchange = vi.fn<(next: Period) => void>();
    const screen = await render(PeriodEditor, {
      period: "",
      issues: [],
      onchange,
    });
    await screen.getByRole("spinbutton", { name: "From" }).fill("2015");
    await screen.getByRole("spinbutton", { name: "To" }).fill("2015");
    expect(onchange).toHaveBeenLastCalledWith(2015);
  });

  it("isolates radio groups across multiple instances (no cross-source collision)", async () => {
    // Two editors on one page. A shared radio `name` would make them ONE document
    // group, so selecting a mode in one would uncheck the other — the bug the
    // per-instance group name fixes. (locators query the whole page, so target the
    // two instances positionally with .nth().)
    await render(PeriodEditor, { period: "", issues: [], onchange: vi.fn() });
    await render(PeriodEditor, { period: 2020, issues: [], onchange: vi.fn() });

    // The mechanism: each instance's radio group has a DISTINCT `name`.
    const yearsRadios = page.getByRole("radio", { name: "Years" }).elements();
    expect(yearsRadios).toHaveLength(2);
    const names = yearsRadios.map((el) => el.getAttribute("name"));
    expect(names[0]).toBeTruthy();
    expect(names[0]).not.toBe(names[1]);

    // The behavior: switching instance A → Token leaves instance B's Years checked.
    await expect
      .element(page.getByRole("radio", { name: "Years" }).nth(1))
      .toBeChecked();
    await page.getByRole("radio", { name: "Token" }).nth(0).click();
    await expect
      .element(page.getByRole("radio", { name: "Years" }).nth(1))
      .toBeChecked();
  });

  it("re-seeds local state when the period prop changes under it (middle-source remove)", async () => {
    // ProjectEditor keys SourceEditor by index, so removing a middle source REUSES
    // this instance for a different source — the `period` prop changes under it.
    // Without the re-seed effect the editor would show the removed source's value.
    const onchange = vi.fn<(next: Period) => void>();
    const screen = await render(PeriodEditor, {
      period: 2015,
      issues: [],
      onchange,
    });
    await expect
      .element(screen.getByRole("spinbutton", { name: "From" }))
      .toHaveValue(2015);

    // The reused instance now belongs to a source with a token period.
    await screen.rerender({ period: "HT2018", issues: [], onchange });

    await expect.element(screen.getByRole("textbox")).toHaveValue("HT2018");
    // Re-seeding is an EXTERNAL change — it must not echo back through onchange.
    expect(onchange).not.toHaveBeenCalled();
  });
});
