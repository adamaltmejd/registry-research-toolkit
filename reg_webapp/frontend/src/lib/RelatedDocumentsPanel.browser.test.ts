import { beforeEach, describe, expect, it, vi } from "vitest";
import { page } from "vitest/browser";
import { render } from "vitest-browser-svelte";
import type { RelatedDocumentsResponse } from "./api";
import { getRelatedDocuments } from "./api";
import RelatedDocumentsPanel from "./RelatedDocumentsPanel.svelte";

vi.mock("./api", async (importOriginal) => {
  const actual = await importOriginal<typeof import("./api")>();
  return {
    ...actual,
    getRelatedDocuments: vi.fn(),
  };
});

function related(
  overrides: Partial<RelatedDocumentsResponse> = {},
): RelatedDocumentsResponse {
  return {
    kind: "related-documents",
    ingested: true,
    register: "lisa",
    documents: [],
    ...overrides,
  };
}

beforeEach(() => {
  vi.mocked(getRelatedDocuments).mockReset();
});

describe("RelatedDocumentsPanel (#742/#967)", () => {
  it("shows an aria-busy loading line while the fetch is pending", async () => {
    vi.mocked(getRelatedDocuments).mockReturnValue(new Promise(() => {}));
    await render(RelatedDocumentsPanel, { register: "lisa" });

    await expect.element(page.getByText("Loading…")).toBeVisible();
    expect(document.querySelector('[aria-busy="true"]')).not.toBeNull();
  });

  it("surfaces a fetch error inline without blanking the heading", async () => {
    vi.mocked(getRelatedDocuments).mockRejectedValue(new Error("network down"));
    await render(RelatedDocumentsPanel, { register: "lisa" });

    await expect
      .element(page.getByRole("alert"))
      .toHaveTextContent("Failed to load source documents");
    await expect
      .element(page.getByRole("heading", { name: "Source documents" }))
      .toBeVisible();
  });

  it("omits the section when no docs DB is ingested", async () => {
    vi.mocked(getRelatedDocuments).mockResolvedValue(
      related({ ingested: false }),
    );
    await render(RelatedDocumentsPanel, { register: "lisa" });

    await expect
      .element(page.getByRole("heading", { name: "Source documents" }))
      .not.toBeInTheDocument();
  });

  it("omits the section when the register has no source documents", async () => {
    vi.mocked(getRelatedDocuments).mockResolvedValue(related());
    await render(RelatedDocumentsPanel, { register: "lisa" });

    await expect
      .element(page.getByRole("heading", { name: "Source documents" }))
      .not.toBeInTheDocument();
  });

  it("renders the PDF link, attribution, and SCB source link", async () => {
    vi.mocked(getRelatedDocuments).mockResolvedValue(
      related({
        documents: [
          {
            title: "LISA register documentation",
            filename: "lisa manual.pdf",
            source_url: "https://www.scb.se/lisa-related",
            license: "CC BY 4.0",
            fetched: "2026-06-01",
            sha256: "a".repeat(64),
            byte_size: 1536,
          },
        ],
      }),
    );
    await render(RelatedDocumentsPanel, { register: "lisa" });

    await expect
      .element(page.getByRole("link", { name: "LISA register documentation" }))
      .toHaveAttribute("href", "/api/docs/file/lisa/lisa%20manual.pdf");
    await expect
      .element(page.getByText(/Källa: SCB · CC BY 4.0/))
      .toBeVisible();
    await expect
      .element(page.getByRole("link", { name: "www.scb.se" }))
      .toHaveAttribute("href", "https://www.scb.se/lisa-related");
    await expect
      .element(page.getByText("1.5 KB · fetched 2026-06-01"))
      .toBeVisible();
  });
});
