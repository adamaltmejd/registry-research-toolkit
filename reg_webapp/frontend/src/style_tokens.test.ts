// Design-token discipline as a deterministic source check (no browser, no LLM).
//
// DESIGN.md → "Visual language" → "Token architecture": components consume SEMANTIC
// roles only. A raw color literal or a raw font stack inside a component's <style>
// block renders identically today and still breaks the light-first, dark-ready
// contract (the [data-theme="dark"] remap cannot reach it). Screenshots cannot catch
// that bypass and biome does not lint <style> blocks inside .svelte, so this test scans
// every .svelte under src/ and fails on the first literal that should be a var(--token).
//
// Exempt by rule, not by allowlist: mask-image declarations (a mask is alpha/luminance
// geometry, not palette, so `#000` there is the only correct spelling). Named colors
// (`transparent`, `currentColor`, the `black`/`white` in a color-mix darkening) are not
// flagged — they are not palette choices either.
import { describe, expect, it } from "vitest";

// Vite's own file glob (no node:fs, no @types/node): every .svelte under src/ as raw
// source, keyed by path relative to this file.
const SVELTE_SOURCES = import.meta.glob<string>("./**/*.svelte", {
  query: "?raw",
  import: "default",
  eager: true,
});

const STYLE_BLOCK = /<style\b[^>]*>([\s\S]*?)<\/style>/g;
const BLOCK_COMMENT = /\/\*[\s\S]*?\*\//g;
// A declaration: property, colon, value up to the next `;` or brace. Rough on purpose —
// it only has to find literals, not parse CSS.
const DECLARATION = /([-\w]+)\s*:\s*([^;{}]+)/g;
const COLOR_LITERAL =
  /#[0-9a-f]{3,8}\b|\b(?:rgba?|hsla?|hwb|oklch|oklab|lab|lch|color)\(/i;

/** Blank out comments while keeping every newline, so line numbers stay true. */
function stripComments(css: string): string {
  return css.replace(BLOCK_COMMENT, (m) => m.replace(/[^\n]/g, " "));
}

function lineOf(text: string, offset: number): number {
  return text.slice(0, offset).split("\n").length;
}

function tokenViolations(source: string, file: string): string[] {
  const out: string[] = [];
  for (const block of source.matchAll(STYLE_BLOCK)) {
    const bodyStart = block.index + block[0].indexOf(block[1]);
    const body = stripComments(block[1]);
    for (const decl of body.matchAll(DECLARATION)) {
      const [, property, value] = decl;
      const prop = property.toLowerCase();
      const line = lineOf(source, bodyStart + decl.index);
      if (!prop.includes("mask") && COLOR_LITERAL.test(value)) {
        out.push(
          `${file}:${line}: raw color in \`${prop}\` — use a semantic var(--token)`,
        );
      }
      if (prop === "font-family" && !value.trim().startsWith("var(")) {
        out.push(
          `${file}:${line}: raw font stack — use var(--font-ui) / var(--font-mono)`,
        );
      }
    }
  }
  return out;
}

describe("design-token discipline (DESIGN.md → Token architecture)", () => {
  it("no .svelte <style> block hardcodes a color or font stack", () => {
    const files = Object.keys(SVELTE_SOURCES).sort();
    expect(files.length).toBeGreaterThan(30); // the glob must actually see the tree
    const violations = files.flatMap((path) =>
      tokenViolations(SVELTE_SOURCES[path], path.replace(/^\.\//, "")),
    );
    expect(violations).toEqual([]);
  });

  it("flags literals and honours the mask exemption", () => {
    const sample = [
      "<div></div>",
      "<style>",
      "  .a { color: #fff; }",
      "  .b { background: rgb(1 2 3 / 50%); }",
      "  .c { mask-image: linear-gradient(to right, #000, transparent); }",
      "  .d { font-family: ui-monospace, monospace; }",
      "  .e { color: var(--text); border: 1px solid var(--border); font-family: var(--font-mono); }",
      "  .f { background: color-mix(in srgb, var(--err) 85%, black); }",
      "</style>",
    ].join("\n");
    expect(
      tokenViolations(sample, "X.svelte").map((v) => v.split(":")[1]),
    ).toEqual(["3", "4", "6"]);
  });
});
