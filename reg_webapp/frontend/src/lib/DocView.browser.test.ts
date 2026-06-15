import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { page } from "vitest/browser";
import { render } from "vitest-browser-svelte";
import type { DocDetail } from "./api";
import { ApiError, getDoc } from "./api";
import DocView from "./DocView.svelte";

// Mock the single GET the viewer drives (mirrors SearchView's api-mock style);
// keep the rest of api.ts real (the type exports + the ApiError class the 404
// branch keys on). Each case stubs `getDoc` and renders `<DocView identifier=… />`.
vi.mock("./api", async (importOriginal) => {
  const actual = await importOriginal<typeof import("./api")>();
  return {
    ...actual,
    getDoc: vi.fn(),
  };
});

// A minimal loaded doc; cases override the fields under test.
function doc(overrides: Partial<DocDetail> = {}): DocDetail {
  return {
    kind: "doc",
    filename: "lisa_kon.md",
    display_name: "LISA — Kön",
    excerpt: "Kön är en bakgrundsvariabel.",
    register: "LISA",
    variable: "Kön",
    source: null,
    source_url: null,
    source_title: null,
    tags: [],
    ...overrides,
  };
}

beforeEach(() => {
  vi.mocked(getDoc).mockReset();
});

afterEach(() => {
  document.body.innerHTML = "";
});

describe("DocView (#394)", () => {
  it("surfaces the backend 404 detail and the LISA-only note", async () => {
    vi.mocked(getDoc).mockRejectedValue(
      new ApiError(
        404,
        { detail: "no documentation for 'x'" },
        "no documentation for 'x'",
      ),
    );
    await render(DocView, { identifier: "x" });

    await expect
      .element(page.getByRole("alert"))
      .toHaveTextContent("no documentation for 'x'");
    await expect
      .element(page.getByText(/coverage is LISA-only today/))
      .toBeVisible();
  });

  it("shows 'No preview available.' when the excerpt is null", async () => {
    vi.mocked(getDoc).mockResolvedValue(doc({ excerpt: null }));
    await render(DocView, { identifier: "lisa_kon.md" });

    await expect.element(page.getByText("No preview available.")).toBeVisible();
  });

  it("renders the excerpt as LITERAL TEXT, never parsed HTML (republication guard)", async () => {
    // `{value}` auto-escapes; an `<b>` in the excerpt must be literal characters,
    // not a parsed element inside the blockquote.
    vi.mocked(getDoc).mockResolvedValue(doc({ excerpt: "foo <b>bar</b>" }));
    await render(DocView, { identifier: "lisa_kon.md" });

    await expect.element(page.getByText("foo <b>bar</b>")).toBeVisible();
    expect(document.querySelector("blockquote b")).toBeNull();
  });

  it("falls back to the filename in the heading when display_name is null", async () => {
    vi.mocked(getDoc).mockResolvedValue(doc({ display_name: null }));
    await render(DocView, { identifier: "lisa_kon.md" });

    await expect
      .element(page.getByRole("heading", { name: "lisa_kon.md" }))
      .toBeVisible();
  });

  it("renders the source pointer as an off-site link, labelled by source_title", async () => {
    vi.mocked(getDoc).mockResolvedValue(
      doc({
        source: "lisa-bakgrundsfakta-1990-2017",
        source_url: "https://scb.se/lisa.pdf",
        source_title: "LISA bakgrundsfakta 1990-2017",
      }),
    );
    await render(DocView, { identifier: "lisa_kon.md" });

    // The title is the preferred label (over the raw source slug); the link
    // opens off-site, so it must be target=_blank + noopener.
    const link = page.getByRole("link", {
      name: "LISA bakgrundsfakta 1990-2017",
    });
    await expect
      .element(link)
      .toHaveAttribute("href", "https://scb.se/lisa.pdf");
    await expect.element(link).toHaveAttribute("target", "_blank");
    await expect.element(link).toHaveAttribute("rel", "noopener noreferrer");
  });

  it("falls back to the source slug as the link label when source_title is null", async () => {
    vi.mocked(getDoc).mockResolvedValue(
      doc({
        source: "lisa-bakgrundsfakta-1990-2017",
        source_url: "https://scb.se/lisa.pdf",
        source_title: null,
      }),
    );
    await render(DocView, { identifier: "lisa_kon.md" });

    await expect
      .element(
        page.getByRole("link", { name: "lisa-bakgrundsfakta-1990-2017" }),
      )
      .toHaveAttribute("href", "https://scb.se/lisa.pdf");
  });

  it("renders the source as plain text (no link) when source_url is null", async () => {
    // An uncurated source has no resolved URL → the identifier shows as text.
    vi.mocked(getDoc).mockResolvedValue(
      doc({ source: "SCB LISA 2021", source_url: null }),
    );
    await render(DocView, { identifier: "lisa_kon.md" });

    await expect.element(page.getByText("SCB LISA 2021")).toBeVisible();
    await expect
      .element(page.getByRole("link", { name: "SCB LISA 2021" }))
      .not.toBeInTheDocument();
  });

  it("renders an alert WITHOUT the LISA note on a non-404 error", async () => {
    vi.mocked(getDoc).mockRejectedValue(
      new ApiError(500, { detail: "internal error" }, "internal error"),
    );
    await render(DocView, { identifier: "lisa_kon.md" });

    await expect
      .element(page.getByRole("alert"))
      .toHaveTextContent("internal error");
    await expect.element(page.getByText(/LISA-only/)).not.toBeInTheDocument();
  });
});
