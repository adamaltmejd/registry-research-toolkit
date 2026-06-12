import { describe, expect, it, vi } from "vitest";
import { render } from "vitest-browser-svelte";
import PeriodPicker from "./PeriodPicker.svelte";

// The catalog PeriodPicker's three input modes (#308 range-first, #338/#340
// Segments for an interrupted series, Text as the wire-grammar escape hatch).
// Self-contained (props in, onsubmit/onclear out) — BindingLeafView owns the
// URL write.
describe("PeriodPicker", () => {
  it("defaults to the range picker and submits the picked wire", async () => {
    const onsubmit = vi.fn<(period: string) => void>();
    const screen = await render(PeriodPicker, {
      period: null,
      onsubmit,
      onclear: vi.fn(),
    });
    await expect
      .element(screen.getByRole("button", { name: "Picker" }))
      .toHaveAttribute("aria-pressed", "true");
    await screen.getByRole("spinbutton", { name: "From" }).fill("2018");
    await screen.getByRole("button", { name: "Apply" }).click();
    expect(onsubmit).toHaveBeenLastCalledWith("2018");
  });

  it("Segments mode builds the #307 comma wire; Apply submits the union query (#340)", async () => {
    const onsubmit = vi.fn<(period: string) => void>();
    const screen = await render(PeriodPicker, {
      period: null,
      onsubmit,
      onclear: vi.fn(),
    });
    await screen.getByRole("button", { name: "Segments" }).click();
    await screen.getByRole("spinbutton", { name: "From" }).fill("2005");
    await screen.getByRole("spinbutton", { name: "To" }).fill("2010");
    await screen.getByRole("button", { name: "Add segment" }).click();
    await screen.getByRole("spinbutton", { name: "From" }).fill("2015");
    await screen.getByRole("spinbutton", { name: "To" }).fill("2020");
    await screen.getByRole("button", { name: "Add segment" }).click();
    await screen.getByRole("button", { name: "Apply" }).click();
    expect(onsubmit).toHaveBeenLastCalledWith("2005..2010,2015..2020");
  });

  it("an active comma ?period opens in Segments with its chips; a removal applies the rest", async () => {
    const onsubmit = vi.fn<(period: string) => void>();
    const screen = await render(PeriodPicker, {
      period: "2005..2010,2015..2020",
      onsubmit,
      onclear: vi.fn(),
    });
    await expect
      .element(screen.getByRole("button", { name: "Segments" }))
      .toHaveAttribute("aria-pressed", "true");
    await expect.element(screen.getByText("2005..2010")).toBeVisible();
    await screen.getByRole("button", { name: "Remove 2005..2010" }).click();
    await screen.getByRole("button", { name: "Apply" }).click();
    expect(onsubmit).toHaveBeenLastCalledWith("2015..2020");
  });

  it("switching to Picker from an unrepresentable active period blanks the buffer — Apply no-ops, never re-submits the invisible value", async () => {
    const onsubmit = vi.fn<(period: string) => void>();
    const screen = await render(PeriodPicker, {
      period: "2005..2010,2015..2020",
      onsubmit,
      onclear: vi.fn(),
    });
    await screen.getByRole("button", { name: "Picker" }).click();
    // Blank controls + a null buffer: Apply must NOT submit the stale comma
    // value hiding behind them (the #347/#349 stale-buffer class).
    await screen.getByRole("button", { name: "Apply" }).click();
    expect(onsubmit).not.toHaveBeenCalled();
  });

  it("an unrepresentable active period (_default) still opens in Text, visible and editable", async () => {
    const screen = await render(PeriodPicker, {
      period: "_default",
      onsubmit: vi.fn(),
      onclear: vi.fn(),
    });
    await expect
      .element(screen.getByRole("button", { name: "Text" }))
      .toHaveAttribute("aria-pressed", "true");
    await expect.element(screen.getByRole("textbox")).toHaveValue("_default");
  });
});
