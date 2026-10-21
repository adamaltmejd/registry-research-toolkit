import { describe, expect, it } from "vitest";
import { matrixAnswerSource, pagesLabel } from "./curated_source";

// The input is the string reg_meta_build's `cis2016_matrix.py` publishes for an
// answer joining two source columns. Fails if the SPA's reading of that grammar
// drifts from the builder's (a line renamed, the page list split differently).
const SHA = "90140a04b0e3e592a0f9b6814aa09005ed655441336747cec5f1cfbe3fc7b0d7";

describe("matrixAnswerSource", () => {
  it("reads the document, digest and per-column pages of a matrix answer", () => {
    const source = matrixAnswerSource(
      [
        "curated:matrix-answer",
        "Document: https://www.scb.se/cis2016.pdf",
        `SHA256: ${SHA}`,
        "Pages: SAMARB_A=14, SAMARB_B=12",
      ].join("\n"),
    );
    expect(source).toEqual({
      documentUrl: "https://www.scb.se/cis2016.pdf",
      sha256: SHA,
      pages: { SAMARB_A: 14, SAMARB_B: 12 },
    });
    expect(pagesLabel([14])).toBe("p. 14");
    expect(pagesLabel([14, 12, 14])).toBe("pp. 12, 14");
  });

  it("refuses another curated head and a malformed locator", () => {
    expect(
      matrixAnswerSource(
        `curated:coding.choice\nDocument: https://x.se/a.pdf\nSHA256: ${SHA}\nPages: A=1`,
      ),
    ).toBeNull();
    expect(
      matrixAnswerSource(
        "curated:matrix-answer\nDocument: https://x.se/a.pdf\nSHA256: abc\nPages: A=1",
      ),
    ).toBeNull();
  });
});
