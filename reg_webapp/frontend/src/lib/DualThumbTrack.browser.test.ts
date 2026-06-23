import { createRawSnippet } from "svelte";
import { describe, expect, it, vi } from "vitest";
import { render } from "vitest-browser-svelte";
import DualThumbTrack from "./DualThumbTrack.svelte";

// The shared dual-thumb-over-a-track primitive (#632) behind both year sliders.
// This verifies the SHARED mechanic only — two clamped non-crossing thumbs, the
// controlled seed/re-sync, the in-track `children` decoration, and the two
// DIVERGENT emit signals (`onLiveInput` per native `input` tick vs `onCommit` on
// native `change`). The consumer-specific concerns (readout, fill geometry,
// coverage/gaps, deviation, clear) live in YearWindowSlider / PeriodWindowSlider
// and are covered by their own suites.

/** Set a range input's value and fire ONLY `input` (a live drag tick). */
function inputTick(el: HTMLInputElement, value: number): void {
  el.value = String(value);
  el.dispatchEvent(new Event("input", { bubbles: true }));
}

/** Fire `change` on a range input (pointer release / keyboard commit). */
function commit(el: HTMLInputElement): void {
  el.dispatchEvent(new Event("change", { bubbles: true }));
}

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

  it("renders the children decoration inside the track, behind the thumbs", async () => {
    const screen = await render(DualThumbTrack, {
      ...base,
      selection: { from: 2000, to: 2010 },
      children: createRawSnippet(() => ({
        render: () => `<div class="fill" data-testid="deco"></div>`,
      })),
    });
    const deco = screen.container.querySelector('[data-testid="deco"]');
    const track = screen.container.querySelector(".track");
    const thumb = screen.container.querySelector(".thumb-from");
    expect(deco).not.toBeNull();
    expect(thumb).not.toBeNull();
    // The decoration is inside the track …
    expect(track?.contains(deco)).toBe(true);
    // … and precedes the thumbs in DOM order (so the thumbs paint over it).
    expect(
      deco!.compareDocumentPosition(thumb!) & Node.DOCUMENT_POSITION_FOLLOWING,
    ).toBeTruthy();
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

  it("onLiveInput fires on every input tick; onCommit only on change (release)", async () => {
    const onLiveInput = vi.fn();
    const onCommit = vi.fn();
    const screen = await render(DualThumbTrack, {
      ...base,
      selection: { from: 2000, to: 2010 },
      onLiveInput,
      onCommit,
    });
    const el = (await screen
      .getByRole("slider", { name: "To year" })
      .element()) as HTMLInputElement;

    // Several live ticks (a drag): onLiveInput fires each time, onCommit never.
    inputTick(el, 2008);
    inputTick(el, 2005);
    inputTick(el, 2003);
    expect(onLiveInput).toHaveBeenCalledTimes(3);
    expect(onCommit).not.toHaveBeenCalled();
    // The thumb reflects the final clamped live value.
    await expect
      .element(screen.getByRole("slider", { name: "To year" }))
      .toHaveValue("2003");

    // Release commits exactly once; no extra live signal.
    commit(el);
    expect(onCommit).toHaveBeenCalledTimes(1);
    expect(onLiveInput).toHaveBeenCalledTimes(3);
  });

  it("works with neither callback wired (both optional)", async () => {
    const screen = await render(DualThumbTrack, {
      ...base,
      selection: { from: 2000, to: 2010 },
    });
    const fromThumb = screen.getByRole("slider", { name: "From year" });
    // No throw, and the thumb still tracks the (clamped) live value.
    await fromThumb.fill("2005");
    await expect.element(fromThumb).toHaveValue("2005");
  });
});
