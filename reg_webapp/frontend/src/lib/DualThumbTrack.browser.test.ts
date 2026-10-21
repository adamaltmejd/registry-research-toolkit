import { describe, expect, it } from "vitest";
import { render } from "vitest-browser-svelte";
import DualThumbTrack from "./DualThumbTrack.svelte";

// The shared dual-thumb-over-a-track primitive (#632) behind both year sliders.
// This verifies the SHARED mechanic only — two clamped non-crossing thumbs, the
// controlled seed/re-sync, the in-track `children` decoration, and the two
// DIVERGENT emit signals (`onLiveInput` per native `input` tick vs `onCommit` on
// native `change`). The consumer-specific concerns (readout, fill geometry,
// coverage/gaps, deviation, clear) live in YearWindowSlider / PeriodWindowSlider
// and are covered by their own suites.

describe("DualThumbTrack", () => {
  const base = { min: 1990, max: 2020 };

  it("renders two slider thumbs seeded (clamped) from the selection", async () => {
    const screen = await render(DualThumbTrack, {
      ...base,
      // Out-of-bounds selection is clamped to the [min, max] bounds by the seed.
      selection: { from: 1980, to: 2030 },
    });
    await expect
      .element(screen.getByRole("slider", { name: "From year" }))
      .toHaveValue("1990");
    await expect
      .element(screen.getByRole("slider", { name: "To year" }))
      .toHaveValue("2020");
  });

  it("clamps so From cannot cross past To", async () => {
    const screen = await render(DualThumbTrack, {
      ...base,
      selection: { from: 2000, to: 2010 },
    });
    const fromThumb = screen.getByRole("slider", { name: "From year" });
    // Drag From past To → clamped to To (no crossed/inverted window).
    await fromThumb.fill("2015");
    await expect.element(fromThumb).toHaveValue("2010");
  });

  it("clamps so To cannot cross before From", async () => {
    const screen = await render(DualThumbTrack, {
      ...base,
      selection: { from: 2000, to: 2010 },
    });
    const toThumb = screen.getByRole("slider", { name: "To year" });
    // Drag To before From → clamped to From.
    await toThumb.fill("1995");
    await expect.element(toThumb).toHaveValue("2000");
  });

  // ── #671 hard-selectable sub-range (opt-in, default = full bounds) ──────────
  describe("selectableMin / selectableMax (#671)", () => {
    it("keeps the native input min/max at the FULL bounds (geometry must not desync)", async () => {
      const screen = await render(DualThumbTrack, {
        ...base, // 1990–2020
        selectableMin: 1995,
        selectableMax: 2015,
        selection: { from: 2000, to: 2010 },
      });
      // The clamp is on the VALUE, not the input range — the native min/max stay
      // the full track so the browser value→pixel mapping matches [min, max].
      const fromThumb = screen.getByRole("slider", { name: "From year" });
      await expect.element(fromThumb).toHaveAttribute("min", "1990");
      await expect.element(fromThumb).toHaveAttribute("max", "2020");
    });

    it("an out-of-coverage seed widens aria-valuemax to include the value (valuenow stays in-range, Fix 6)", async () => {
      const screen = await render(DualThumbTrack, {
        ...base, // 1990–2020
        selectableMin: 1995,
        selectableMax: 2015,
        // An explicit out-of-coverage `?period` seeds To beyond selectableMax
        // (the seed clamps only to [min, max] → To = 2018, honest render).
        selection: { from: 2000, to: 2018 },
      });
      const toThumb = screen.getByRole("slider", { name: "To year" });
      // The seeded value renders honestly past the selectable ceiling…
      await expect.element(toThumb).toHaveValue("2018");
      // …and aria-valuemax WIDENS to >= it (2018), so aria-valuenow (2018) is
      // never outside [valuemin, valuemax] — valid ARIA, not the smaller selHi.
      await expect.element(toThumb).toHaveAttribute("aria-valuemax", "2018");
      // The From thumb (in-range) is unaffected — still the selectable floor.
      await expect
        .element(screen.getByRole("slider", { name: "From year" }))
        .toHaveAttribute("aria-valuemin", "1995");
    });
  });
});
