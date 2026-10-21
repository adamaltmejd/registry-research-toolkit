// A state's `curated:matrix-answer` provenance, as the builder publishes it
// (reg_meta_build `cis2016_matrix.py`): the head, then the cited document's URL, its
// SHA256 and the page of each source column.
//
//   curated:matrix-answer
//   Document: https://www.scb.se/…/cis2016.pdf
//   SHA256: 90140a04…
//   Pages: INNO_A=12, INNO_B=13
//
// The reader serves the string as is, so the SPA parses it here. Anything else,
// or a malformed locator line, is not a matrix answer: `null`.

export type MatrixAnswerSource = {
  documentUrl: string;
  sha256: string;
  /** Source column → page in the document. */
  pages: Record<string, number>;
};

const HEAD = "curated:matrix-answer";

export function matrixAnswerSource(
  provenance: string | null | undefined,
): MatrixAnswerSource | null {
  const lines = provenance?.split("\n") ?? [];
  if (lines.length !== 4 || lines[0] !== HEAD) {
    return null;
  }
  const [, document, digest, pageLine] = lines;
  const documentUrl = document?.match(/^Document: (https?:\/\/\S+)$/)?.[1];
  const sha256 = digest?.match(/^SHA256: ([0-9a-f]{64})$/)?.[1];
  const pageList = pageLine?.match(/^Pages: (.+)$/)?.[1];
  if (!documentUrl || !sha256 || !pageList) {
    return null;
  }
  const pages: Record<string, number> = {};
  for (const entry of pageList.split(", ")) {
    const match = entry.match(/^(.+)=([1-9]\d*)$/);
    if (!match?.[1] || !match[2]) {
      return null;
    }
    pages[match[1]] = Number(match[2]);
  }
  return { documentUrl, sha256, pages };
}

/** "p. 12" for one page, "pp. 12, 14" for several distinct pages. */
export function pagesLabel(pages: number[]): string {
  const distinct = [...new Set(pages)].sort((a, b) => a - b);
  return distinct.length === 1
    ? `p. ${distinct[0]}`
    : `pp. ${distinct.join(", ")}`;
}
