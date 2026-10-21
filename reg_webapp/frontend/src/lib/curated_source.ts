// A state's `curated:matrix-answer` provenance, as the builder publishes it
// (reg_meta_build `cis2016_matrix.py`): the head, then the cited document's URL, its
// SHA256 and the page of each source column.
//
//   curated:matrix-answer
//   Document: https://www.scb.se/…/cis2016.pdf
//   SHA256: 90140a04…
//   Pages: INNO_A=12, INNO_B=13
//
// A state's provenance joins its sorted blocks with a blank line (no block holds
// one), so the matrix answer may sit beside e.g. a `curated:coding.choice` block.
// The reader serves the string as is, so the SPA parses it here. No matrix-answer
// block, or a malformed one, is not a matrix answer: `null`.

export type MatrixAnswerSource = {
  documentUrl: string;
  sha256: string;
  /** Source column → page in the document. */
  pages: Record<string, number>;
};

const HEAD = "curated:matrix-answer";

/** A state's provenance blocks, each a head line plus its locator lines. */
export function provenanceBlocks(
  provenance: string | null | undefined,
): string[] {
  return provenance ? provenance.split("\n\n") : [];
}

export function matrixAnswerSource(
  provenance: string | null | undefined,
): MatrixAnswerSource | null {
  const lines =
    provenanceBlocks(provenance)
      .map((block) => block.split("\n"))
      .find((blockLines) => blockLines[0] === HEAD) ?? [];
  if (lines.length !== 4) {
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

/** "p. 12" for one page, "pp. 12, 14" for several distinct pages; a no-break
 * space keeps "p." with its number. */
export function pagesLabel(pages: number[]): string {
  const distinct = [...new Set(pages)].sort((a, b) => a - b);
  return distinct.length === 1
    ? `p. ${distinct[0]}`
    : `pp. ${distinct.join(", ")}`;
}
