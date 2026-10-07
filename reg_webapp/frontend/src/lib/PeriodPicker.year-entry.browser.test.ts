import { describe, expect, it, vi } from "vitest";
import { userEvent } from "vitest/browser";
import { render } from "vitest-browser-svelte";
import PeriodPicker from "./PeriodPicker.svelte";
import type { Coverage } from "./period";
import type { StudyWindow } from "./project_data";

// Split from PeriodPicker.browser.test.ts by contract surface: exact year entry
// and keyboard focus across an applied period. Sibling: PeriodPicker.browser.test.ts.

// The EXACT year entry (Y-16). The reported defect: from 2019..2020, moving the
// From thumb to 2022 first clamps it to 2020 (DualThumbTrack's non-crossing
// clamp), so moving To to 2022 next submits 2020..2022 — a wider request than
// the researcher asked for, with no signal. The two year fields are the atomic
// path: both bounds are read together on Apply, so the ORDER the user types them
// in cannot change the submitted wire, and an entry that crosses or falls outside
// the selectable years is refused with a reason instead of clamped.
// A leaf still being delivered (open-ended coverage from 2015) on a 2024
// catalog, narrowed to 2019..2020 — the reported starting state of both the
// exact-entry (Y-16) and the keyboard-focus (Y-65) cases below. Selectable years
// are the coverage band 2015–2024, for the thumbs and the fields alike.
const EXACT = {
  period: "2019..2020",
  window: null,
  coverage: { from: 2015, to: null } as Coverage,
  vintageYear: 2024,
};

/** The three controls these cases drive, off one render. */
function fields(
  screen: Awaited<ReturnType<typeof render<typeof PeriodPicker>>>,
) {
  return {
    from: screen.getByRole("textbox", { name: "From" }),
    to: screen.getByRole("textbox", { name: "To" }),
    apply: screen.getByRole("button", { name: "Apply period" }),
  };
}

describe("PeriodPicker — exact year entry (Y-16)", () => {
  it("collapses a range to a single year — the reported LOWER-FIRST sequence", async () => {
    const onsubmit = vi.fn<(period: string) => void>();
    const screen = await render(PeriodPicker, {
      ...EXACT,
      onsubmit,
      onclear: vi.fn(),
    });
    const { from, to, apply } = fields(screen);
    // The seed is the active 2019..2020, mirrored into the fields.
    await expect.element(from).toHaveValue("2019");
    await expect.element(to).toHaveValue("2020");

    // Type the LOWER bound first — the order that silently produced 2020..2022
    // on the slider. It is NOT clamped to 2020: the field keeps 2022, Apply is
    // refused, and the reason names the crossing.
    await from.fill("2022");
    await apply.click();
    expect(onsubmit).not.toHaveBeenCalled();
    await expect.element(from).toHaveValue("2022");
    await expect
      .element(
        screen.getByText(
          "From 2022 is after To 2020 — enter From at or before To.",
        ),
      )
      .toBeVisible();

    // Completing the pair applies the single year the researcher asked for.
    await to.fill("2022");
    await apply.click();
    expect(onsubmit).toHaveBeenCalledOnce();
    expect(onsubmit).toHaveBeenLastCalledWith("2022");
  });

  it("widens a range in one Apply and shows the pending range before it", async () => {
    const onsubmit = vi.fn<(period: string) => void>();
    const screen = await render(PeriodPicker, {
      ...EXACT,
      onsubmit,
      onclear: vi.fn(),
    });
    const { from, to, apply } = fields(screen);
    await from.fill("2016");
    await to.fill("2023");
    // The pending range is on screen — the slider readout and its thumbs follow
    // the typed entry — and nothing has been submitted yet.
    await expect
      .element(screen.getByText("2016–2023", { exact: true }))
      .toBeVisible();
    await expect
      .element(screen.getByRole("slider", { name: "From year" }))
      .toHaveValue("2016");
    await expect
      .element(screen.getByRole("slider", { name: "To year" }))
      .toHaveValue("2023");
    expect(onsubmit).not.toHaveBeenCalled();

    await apply.click();
    expect(onsubmit).toHaveBeenLastCalledWith("2016..2023");
  });

  it("refuses a year outside the selectable coverage band and keeps the pending range", async () => {
    const onsubmit = vi.fn<(period: string) => void>();
    const screen = await render(PeriodPicker, {
      ...EXACT,
      onsubmit,
      onclear: vi.fn(),
    });
    const { from, to, apply } = fields(screen);
    // 2010 predates the delivered years the thumbs are hard-clamped to (#671).
    await from.fill("2010");
    await apply.click();
    expect(onsubmit).not.toHaveBeenCalled();
    // Only the year at fault is marked — To still holds a good year.
    await expect.element(from).toHaveAttribute("aria-invalid", "true");
    await expect.element(to).toHaveAttribute("aria-invalid", "false");
    await expect
      .element(
        screen.getByText(
          "2010 is outside 2015–2024 — pick a year in that range.",
        ),
      )
      .toBeVisible();
    // The refusal changed nothing: the field still shows what was typed and the
    // pending range is still the active 2019–2020.
    await expect.element(from).toHaveValue("2010");
    await expect
      .element(screen.getByText("2019–2020", { exact: true }))
      .toBeVisible();
  });

  it("refuses enforced steward bounds the same way, on both sides (#1037)", async () => {
    const onsubmit = vi.fn<(period: string) => void>();
    const screen = await render(PeriodPicker, {
      period: "2004..2006",
      window: null,
      coverage: { from: 1990, to: 2030 } as Coverage, // clipped to the steward bounds
      windowMinYear: 2000,
      vintageYear: 2010,
      enforcePeriodBounds: true,
      onsubmit,
      onclear: vi.fn(),
    });
    const { from, to, apply } = fields(screen);
    await from.fill("1995");
    await apply.click();
    expect(onsubmit).not.toHaveBeenCalled();
    await expect
      .element(
        screen.getByText(
          "1995 is outside 2000–2010 — pick a year in that range.",
        ),
      )
      .toBeVisible();

    await from.fill("2001");
    await to.fill("2012");
    await apply.click();
    expect(onsubmit).not.toHaveBeenCalled();
    await expect
      .element(
        screen.getByText(
          "2012 is outside 2000–2010 — pick a year in that range.",
        ),
      )
      .toBeVisible();

    // Inside the steward bounds it applies normally.
    await to.fill("2009");
    await apply.click();
    expect(onsubmit).toHaveBeenLastCalledWith("2001..2009");
  });

  it("names BOTH fields when neither holds a year (Y-17)", async () => {
    const onsubmit = vi.fn<(period: string) => void>();
    const screen = await render(PeriodPicker, {
      ...EXACT,
      onsubmit,
      onclear: vi.fn(),
    });
    const { from, to, apply } = fields(screen);
    // Clear both and press Apply — the reported case. Both hairlines go red and
    // both share ONE description, so naming only From leaves To unexplained.
    await from.fill("");
    await to.fill("");
    await apply.click();
    expect(onsubmit).not.toHaveBeenCalled();
    await expect.element(from).toHaveAttribute("aria-invalid", "true");
    await expect.element(to).toHaveAttribute("aria-invalid", "true");
    await expect
      .element(
        screen.getByText(
          "From and To must each be a four-digit year, like 2015.",
        ),
      )
      .toBeVisible();
    // That one description is what BOTH fields point at — the reason it has to
    // name both of them.
    const described = from.element().getAttribute("aria-describedby") ?? "";
    expect(document.getElementById(described)?.textContent).toContain(
      "From and To",
    );
    await expect.element(to).toHaveAttribute("aria-describedby", described);

    // Completing one side alone leaves the other's refusal on its own field.
    await from.fill("2016");
    await expect
      .element(screen.getByText("To must be a four-digit year, like 2015."))
      .toBeVisible();
    await expect.element(from).toHaveAttribute("aria-invalid", "false");
  });

  it("names BOTH years when both bounds fall outside the band", async () => {
    const onsubmit = vi.fn<(period: string) => void>();
    const screen = await render(PeriodPicker, {
      ...EXACT,
      onsubmit,
      onclear: vi.fn(),
    });
    const { from, to, apply } = fields(screen);
    await from.fill("2010");
    await to.fill("2030");
    await apply.click();
    expect(onsubmit).not.toHaveBeenCalled();
    // Both hairlines are red, so both years have to be accounted for.
    await expect.element(from).toHaveAttribute("aria-invalid", "true");
    await expect.element(to).toHaveAttribute("aria-invalid", "true");
    await expect
      .element(
        screen.getByText(
          "2010 and 2030 are outside 2015–2024 — pick years in that range.",
        ),
      )
      .toBeVisible();
  });

  it("a thumb drag after a typed entry takes the fields back over", async () => {
    const onsubmit = vi.fn<(period: string) => void>();
    const screen = await render(PeriodPicker, {
      ...EXACT,
      onsubmit,
      onclear: vi.fn(),
    });
    const { from, to, apply } = fields(screen);
    await from.fill("2016");
    await to.fill("2023");
    // Dragging is still authoritative: the fields mirror the thumbs again.
    await screen.getByRole("slider", { name: "From year" }).fill("2018");
    await expect.element(from).toHaveValue("2018");
    await expect.element(to).toHaveValue("2023");
    await apply.click();
    expect(onsubmit).toHaveBeenLastCalledWith("2018..2023");
  });

  it("an exact entry replaces a sub-annual ?period the slider cannot author", async () => {
    const onsubmit = vi.fn<(period: string) => void>();
    const screen = await render(PeriodPicker, {
      period: "HT2020",
      window: { from: 2000, to: 2010 } as StudyWindow,
      onsubmit,
      onclear: vi.fn(),
    });
    const { from, to, apply } = fields(screen);
    // Untouched Apply still refuses to rewrite it (the #615 guard)…
    await apply.click();
    expect(onsubmit).not.toHaveBeenCalled();
    // …but an explicit typed range is an explicit request, so it authors.
    await from.fill("2005");
    await to.fill("2006");
    await apply.click();
    expect(onsubmit).toHaveBeenLastCalledWith("2005..2006");
  });

  // A re-render is not a re-seed. Both consumers rebuild these props from derived
  // state — the leaf's `coverageFromStates(node.states)`, the group's
  // `unionCoverage` over its selectable bands, a window store that rewrites the
  // same span — so an unrelated recompute hands the picker equal-but-NEW objects
  // while the user is mid-entry. Nothing on screen moved, so nothing the user is
  // authoring may be thrown away.
  const CHURN = {
    period: null,
    window: { from: 2000, to: 2010 } as StudyWindow,
    coverage: { from: 1995, to: 2008 } as Coverage,
  };

  it("keeps a half-typed entry through a re-render that only changes object identity", async () => {
    const onsubmit = vi.fn<(period: string) => void>();
    const props = { ...CHURN, onsubmit, onclear: vi.fn() };
    const screen = await render(PeriodPicker, props);
    const { from, to, apply } = fields(screen);
    await from.fill("2002");
    await to.fill("2006");
    // Same values, new objects — as a parent recompute delivers them.
    await screen.rerender({
      ...props,
      window: { from: 2000, to: 2010 },
      coverage: { from: 1995, to: 2008 },
    });
    await expect.element(from).toHaveValue("2002");
    await expect.element(to).toHaveValue("2006");
    await apply.click();
    expect(onsubmit).toHaveBeenLastCalledWith("2002..2006");
  });

  // A moved SELECTABLE BAND is a re-seed even when the seeded years stand still.
  // Coverage narrowing by one year, or hard steward bounds arriving, leaves
  // `?period`, the active window, the ceiling AND the seed all equal — so the
  // value comparison that protects the churn cases above holds — while the years
  // the user may actually pick move under an already-authored value. That value is
  // stale: it re-seeds, and Apply sends what the readout, the thumbs and the
  // fields show. It is never clamped into a nearby range (the silent clamp this
  // ticket removes) and never submitted from behind the display.
  const BAND = { ...CHURN, vintageYear: 2026 };

  /** Drag both thumbs to the coverage floor..2006 — legal now, not after. */
  async function dragToFloor(
    screen: Awaited<ReturnType<typeof render<typeof PeriodPicker>>>,
  ): Promise<void> {
    await screen.getByRole("slider", { name: "From year" }).fill("1995");
    await screen.getByRole("slider", { name: "To year" }).fill("2006");
    await expect
      .element(screen.getByText("1995–2006", { exact: true }))
      .toBeVisible();
  }

  it("re-seeds a dragged window the moved COVERAGE floor no longer allows", async () => {
    const onsubmit = vi.fn<(period: string) => void>();
    const props = { ...BAND, onsubmit, onclear: vi.fn() };
    const screen = await render(PeriodPicker, props);
    const { from, to, apply } = fields(screen);
    await dragToFloor(screen);
    // Coverage now starts in 1996. Period, window, ceiling and the window∩coverage
    // seed (2000–2008) are all unchanged — only the selectable band moved.
    await screen.rerender({ ...props, coverage: { from: 1996, to: 2008 } });
    // The 1995 the user dragged to is no longer selectable, so the picker is back
    // on its seed — in the readout, the thumbs and the fields alike.
    await expect
      .element(screen.getByText("2000–2008", { exact: true }))
      .toBeVisible();
    await expect
      .element(screen.getByRole("slider", { name: "From year" }))
      .toHaveValue("2000");
    await expect.element(from).toHaveValue("2000");
    await expect.element(to).toHaveValue("2008");
    // …and Apply submits that, not the sub-floor drag.
    await apply.click();
    expect(onsubmit).toHaveBeenLastCalledWith("2000..2008");
  });

  it("re-seeds a TYPED entry the moved floor invalidates, and never re-submits it", async () => {
    const onsubmit = vi.fn<(period: string) => void>();
    const props = { ...BAND, onsubmit, onclear: vi.fn() };
    const screen = await render(PeriodPicker, props);
    const { from, to, apply } = fields(screen);
    await from.fill("1995");
    await to.fill("2006");
    await screen.rerender({ ...props, coverage: { from: 1996, to: 2008 } });
    // The typed pair went with the band it was typed against: the fields mirror
    // the re-seeded selection again…
    await expect.element(from).toHaveValue("2000");
    await expect.element(to).toHaveValue("2008");
    // …and Apply sends that, not the sub-floor pair it was carrying. (A FRESH
    // below-floor entry is still refused, with the reason — see the band and
    // steward-bound refusals above.)
    await apply.click();
    expect(onsubmit).toHaveBeenLastCalledWith("2000..2008");
  });

  it("re-seeds on a WIDENED band too — the key is the band's value, not one edge", async () => {
    const onsubmit = vi.fn<(period: string) => void>();
    const props = { ...BAND, onsubmit, onclear: vi.fn() };
    const screen = await render(PeriodPicker, props);
    await dragToFloor(screen);
    // Coverage back to 1990 — the drag would still be selectable, but the band
    // MOVED, which only happens when the subject or the deployment bounds change
    // underneath (a leaf navigation shares the URL; Fix C). The buffer belongs to
    // the band it was authored against either way, so it re-seeds here too rather
    // than carrying the previous leaf's span into this one.
    await screen.rerender({ ...props, coverage: { from: 1990, to: 2008 } });
    await expect
      .element(screen.getByText("2000–2008", { exact: true }))
      .toBeVisible();
    await screen.getByRole("button", { name: "Apply period" }).click();
    expect(onsubmit).toHaveBeenLastCalledWith("2000..2008");
  });
});

// The reported Y-65 flow: a keyboard researcher adjusts the period repeatedly
// through the From and To fields on /catalog/scb/lisa/kon?period=2019..2020. The
// route no longer remounts the card around them (the router keeps its route
// object across a query-only navigation), so what has to hold here is the
// picker's own half: applying a period must not remount anything INSIDE the card
// either, or the control that submitted loses focus just the same.
describe("PeriodPicker — keyboard focus across an applied period (Y-65)", () => {
  it("submits the typed pair on Enter and the field keeps focus and value when it arrives back", async () => {
    const onsubmit = vi.fn<(period: string) => void>();
    const props = { ...EXACT, onsubmit, onclear: vi.fn() };
    const screen = await render(PeriodPicker, props);
    const { from, to } = fields(screen);
    await from.fill("2018");
    await to.fill("2021");
    const field = to.element() as HTMLInputElement;
    field.focus();
    // Enter in a year field is the implicit submit of the card's own form — the
    // Apply path this consumer actually uses.
    await userEvent.keyboard("{Enter}");
    expect(onsubmit).toHaveBeenLastCalledWith("2018..2021");

    // The consumer writes the URL and the applied period arrives back as a prop.
    await screen.rerender({ ...props, period: "2018..2021" });
    expect(to.element()).toBe(field);
    expect(document.activeElement).toBe(field);
    await expect.element(to).toHaveValue("2021");
  });

  it("keeps the slider thumbs mounted across the same arrival, so a thumb Apply holds focus", async () => {
    const props = { ...EXACT, onsubmit: vi.fn(), onclear: vi.fn() };
    const screen = await render(PeriodPicker, props);
    const thumb = screen.getByRole("slider", { name: "From year" });
    const knob = thumb.element() as HTMLInputElement;
    knob.focus();
    await screen.rerender({ ...props, period: "2018..2021" });
    // The SAME node, re-seeded in place by DualThumbTrack's controlled re-sync —
    // not a fresh one that took the focus with it when the old one went away.
    expect(thumb.element()).toBe(knob);
    expect(document.activeElement).toBe(knob);
    await expect.element(thumb).toHaveValue("2018");
  });
});
