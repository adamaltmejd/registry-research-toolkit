import { describe, expect, it, vi } from "vitest";
import { render } from "vitest-browser-svelte";
import type { StudyWindow } from "./project_data";
import YearWindowSlider from "./YearWindowSlider.svelte";

// The header dual-thumb year slider (#611 → Period model). Self-contained: bounds
// + active window in, `onchange` out. The window runtime layer wiring lives in
// App.svelte; this verifies the control's own behavior (two thumbs, bounds,
// readout, clamped emit).
describe("YearWindowSlider", () => {
  it("renders two slider thumbs seeded at the bounds when no window is set", async () => {
    const screen = await render(YearWindowSlider, {
      min: 1960,
      max: 2026,
      window: null,
      onchange: vi.fn(),
    });
    const fromThumb = screen.getByRole("slider", { name: "From year" });
    const toThumb = screen.getByRole("slider", { name: "To year" });
    await expect.element(fromThumb).toHaveValue("1960");
    await expect.element(toThumb).toHaveValue("2026");
    // No explicit window → the readout reads "full history", not a year span.
    await expect.element(screen.getByText("full history")).toBeVisible();
  });

  it("seeds the thumbs + readout from an active window", async () => {
    const screen = await render(YearWindowSlider, {
      min: 1960,
      max: 2026,
      window: { from: 1990, to: 2010 },
      onchange: vi.fn(),
    });
    await expect
      .element(screen.getByRole("slider", { name: "From year" }))
      .toHaveValue("1990");
    await expect
      .element(screen.getByRole("slider", { name: "To year" }))
      .toHaveValue("2010");
    await expect.element(screen.getByText("1990–2010")).toBeVisible();
  });

  it("moving the From thumb emits the new window", async () => {
    const onchange = vi.fn<(next: StudyWindow) => void>();
    const screen = await render(YearWindowSlider, {
      min: 1960,
      max: 2026,
      window: { from: 1990, to: 2010 },
      onchange,
    });
    await screen.getByRole("slider", { name: "From year" }).fill("1995");
    expect(onchange).toHaveBeenLastCalledWith({ from: 1995, to: 2010 });
  });

  it("moving the To thumb emits the new window", async () => {
    const onchange = vi.fn<(next: StudyWindow) => void>();
    const screen = await render(YearWindowSlider, {
      min: 1960,
      max: 2026,
      window: { from: 1990, to: 2010 },
      onchange,
    });
    await screen.getByRole("slider", { name: "To year" }).fill("2005");
    expect(onchange).toHaveBeenLastCalledWith({ from: 1990, to: 2005 });
  });

  it("clamps so From cannot cross past To", async () => {
    const onchange = vi.fn<(next: StudyWindow) => void>();
    const screen = await render(YearWindowSlider, {
      min: 1960,
      max: 2026,
      window: { from: 1990, to: 2000 },
      onchange,
    });
    // Drag From past To → it's clamped to To (no crossed/inverted window).
    await screen.getByRole("slider", { name: "From year" }).fill("2010");
    expect(onchange).toHaveBeenLastCalledWith({ from: 2000, to: 2000 });
  });
});
