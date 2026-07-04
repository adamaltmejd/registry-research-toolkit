import { describe, expect, it, vi } from "vitest";
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
      status: "warnings",
      requestError: null,
      windowHints: [],
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
      status: "warnings",
      requestError: null,
      windowHints: [],
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

  it("links a source-level finding to the REGISTER catalog page (2-seg prefix, #993)", async () => {
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
      status: "errors",
      requestError: null,
      windowHints: [],
      sources: SOURCES,
    });

    // The source-level link targets the 2-seg provider/register page, NOT the
    // 3-seg register_variant (a variant slug is a query axis, not a node — #993).
    const link = page.getByRole("link", { name: /Fix in catalog/ });
    await expect.element(link).toBeVisible();
    await expect.element(link).toHaveAttribute("href", "/catalog/scb/lisa");
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
        status: "warnings",
        requestError: null,
        windowHints: [],
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
      status: "errors",
      requestError: null,
      windowHints: [],
      sources: SOURCES,
    });

    // No source card to locate → the raw pointer is shown (and there's no locate
    // button for this finding). `sourceAnchorId` is unused here but pins that a
    // top-level path does NOT resolve to a card.
    expect(sourceAnchorId(0)).toBe("loc-source-0");
    await expect.element(page.getByText("/name")).toBeVisible();
  });

  it("shows the automatic checking state while validation is pending", async () => {
    await render(ValidationPanel, {
      result: null,
      status: "checking",
      requestError: null,
      windowHints: [],
      sources: SOURCES,
    });

    await expect
      .element(page.getByText("Checking the current project…"))
      .toBeVisible();
    expect(document.querySelector('[aria-busy="true"]')).not.toBeNull();
  });

  it("shows project-window coverage hints without mixing them into validation issues", async () => {
    await render(ValidationPanel, {
      result: { ok: true, issues: [] },
      status: "ok",
      requestError: null,
      windowHints: [
        {
          label: "Source 'lisa_main'",
          message:
            "Source 'lisa_main' ends in 2018, before your study window ends 2020.",
          catalogHref: "/catalog/scb/lisa",
          catalogLabel: "scb/lisa",
        },
      ],
      sources: SOURCES,
    });

    await expect
      .element(
        page.getByText(
          "Source 'lisa_main' ends in 2018, before your study window ends 2020.",
        ),
      )
      .toBeVisible();
    const link = page.getByRole("link", { name: /Extend in catalog/ });
    await expect.element(link).toBeVisible();
    await expect.element(link).toHaveAttribute("href", "/catalog/scb/lisa");
    expect(document.body.textContent).not.toContain("window_coverage");
  });

  it("offers a retry action when the validation request itself fails", async () => {
    const onRetry = vi.fn();
    await render(ValidationPanel, {
      result: null,
      status: "unchecked",
      requestError: "request body is not a JSON object",
      windowHints: [],
      sources: SOURCES,
      onRetry,
    });

    const retry = page.getByRole("button", { name: "Retry validation" });
    await expect.element(retry).toBeVisible();
    await retry.click();
    expect(onRetry).toHaveBeenCalledOnce();
  });

  it("renders window hints for duplicate source labels without crashing", async () => {
    await render(ValidationPanel, {
      result: { ok: false, issues: [] },
      status: "errors",
      requestError: null,
      windowHints: [
        {
          label: "Source 'dup'",
          message:
            "Source 'dup' ends in 2018, before your study window ends 2020.",
          catalogHref: "/catalog/scb/lisa",
          catalogLabel: "scb/lisa",
        },
        {
          label: "Source 'dup'",
          message:
            "Source 'dup' ends in 2019, before your study window ends 2020.",
          catalogHref: "/catalog/scb/rams",
          catalogLabel: "scb/rams",
        },
      ],
      sources: SOURCES,
    });

    expect(
      page.getByRole("link", { name: /Extend in catalog/ }).elements(),
    ).toHaveLength(2);
  });
});
