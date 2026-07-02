import { describe, expect, it } from "vitest";
import { page } from "vitest/browser";
import { render } from "vitest-browser-svelte";
import ValidationPanel from "./ValidationPanel.svelte";
import { bindingAnchorId, sourceAnchorId } from "./validation";

// PR D2 (UI audit finding 6): the findings summary must speak researcher —
// human TITLE leads, the raw `code` is demoted but visible, and the locator is a
// click-to-scroll LABEL (not a raw JSON pointer). These browser tests drive the
// panel directly with a fabricated /validate result + the draft's `sources`.

const SOURCES = [
  {
    name: "lisa_main",
    register_variant: "scb/lisa/v1",
    bindings: [{ variable: "scb/lisa/adeldag" }],
  },
];

const DRIFT_RESULT = {
  ok: true,
  issues: [
    {
      level: "info" as const,
      code: "binding_state_drifts_within_period",
      path: "/sources/0/bindings/0/variable",
      message:
        "binding 'scb/lisa/adeldag' spans 2 states across a transition within period 2015..2020",
    },
  ],
};

describe("ValidationPanel — researcher-language findings", () => {
  it("leads with the human title, demotes the raw code, never shows the raw pointer", async () => {
    await render(ValidationPanel, {
      result: DRIFT_RESULT,
      requestError: null,
      sources: SOURCES,
    });

    // Human title (codeLabel) is shown…
    await expect
      .element(
        page.getByText("The period crosses a state transition", {
          exact: false,
        }),
      )
      .toBeVisible();
    // …the raw code stays visible but demoted (still rendered as the stable id)…
    await expect
      .element(page.getByText("binding_state_drifts_within_period"))
      .toBeVisible();
    // …the location reads in user terms, NOT the raw JSON pointer…
    await expect
      .element(page.getByText("Source 'lisa_main' → binding scb/lisa/adeldag"))
      .toBeVisible();
    // …and the raw pointer is NOT rendered anywhere.
    expect(document.body.textContent).not.toContain(
      "/sources/0/bindings/0/variable",
    );
    // …and the message never leaks a dataclass repr.
    expect(document.body.textContent).not.toContain("PeriodRange");
  });

  it("links out to the binding's catalog subject page (read-only cart fixes happen in the browser, #991)", async () => {
    await render(ValidationPanel, {
      result: DRIFT_RESULT,
      requestError: null,
      sources: SOURCES,
    });

    // The binding-level finding carries an outbound catalog link to its variable's
    // subject page — the only place the binding is re-picked.
    const link = page.getByRole("link", { name: /Fix in catalog/ });
    await expect.element(link).toBeVisible();
    await expect
      .element(link)
      .toHaveAttribute("href", "/catalog/scb/lisa/adeldag");
  });

  it("links a source-level finding to the register/variant catalog page", async () => {
    await render(ValidationPanel, {
      result: {
        ok: false,
        issues: [
          {
            level: "error" as const,
            code: "empty_bindings",
            path: "/sources/0/bindings",
            message: "source has no bindings",
          },
        ],
      },
      requestError: null,
      sources: SOURCES,
    });

    const link = page.getByRole("link", { name: /Fix in catalog/ });
    await expect.element(link).toBeVisible();
    await expect.element(link).toHaveAttribute("href", "/catalog/scb/lisa/v1");
  });

  it("clicking the location label flashes the target binding card", async () => {
    // Place a stand-in binding card with the anchor id the panel will resolve to,
    // mirroring what BindingEditor mounts in the real tree.
    const card = document.createElement("div");
    card.id = bindingAnchorId(0, 0);
    card.textContent = "binding card";
    document.body.appendChild(card);

    try {
      await render(ValidationPanel, {
        result: DRIFT_RESULT,
        requestError: null,
        sources: SOURCES,
      });

      const locate = page.getByRole("button", {
        name: "Source 'lisa_main' → binding scb/lisa/adeldag",
      });
      await expect.element(locate).toBeVisible();
      expect(card.classList.contains("locate-flash")).toBe(false);

      await locate.click();
      expect(card.classList.contains("locate-flash")).toBe(true);
    } finally {
      card.remove();
    }
  });

  it("falls back to the raw pointer when the path names no source card", async () => {
    await render(ValidationPanel, {
      result: {
        ok: false,
        issues: [
          {
            level: "error" as const,
            code: "missing_required_field",
            path: "/name",
            message: "missing required field 'name'",
          },
        ],
      },
      requestError: null,
      sources: SOURCES,
    });

    // No source card to locate → the raw pointer is shown (and there's no locate
    // button for this finding). `sourceAnchorId` is unused here but pins that a
    // top-level path does NOT resolve to a card.
    expect(sourceAnchorId(0)).toBe("loc-source-0");
    await expect.element(page.getByText("/name")).toBeVisible();
  });
});
