// Design-token source lints, run by `bun run lint:tokens` (part of `bun run lint`).
// Deterministic source checks, not behavior tests: no browser, no LLM.
//
// 1. Token discipline. DESIGN.md → "Visual language" → "Token architecture":
//    components consume SEMANTIC roles only. A raw color literal or a raw font stack
//    inside a component's <style> block renders identically today and still breaks
//    the light-first, dark-ready contract (the [data-theme="dark"] remap cannot reach
//    it). Screenshots cannot catch that bypass and biome does not lint <style> blocks
//    inside .svelte, so this scans every .svelte under src/ for a literal that should
//    be a var(--token). Exempt by rule, not by allowlist: mask-image declarations (a
//    mask is alpha/luminance geometry, not palette, so `#000` there is the only
//    correct spelling). Named colors (`transparent`, `currentColor`, the
//    `black`/`white` in a color-mix darkening) are not flagged — they are not palette
//    choices either.
//
// 2. DESIGN.md ↔ tokens.css parity. `DESIGN.md` is the normative design language (the
//    DESIGN.md format: the YAML front matter IS the token set). `lint:design` checks
//    that file's own shape; this checks the other half — that `src/tokens.css`
//    actually RESOLVES to those values, so a color, radius, spacing step or type size
//    can only move by moving the spec first. The spec side is read through the same
//    pinned `@google/design.md` the lint:design script runs: `lint()` hands back the
//    front matter already resolved — `{a.b}` references followed, colors normalized
//    to hex — so this file only resolves the OTHER side, the `var(--x)` chains in the
//    `:root` block.
import { lint, type ResolvedDimension } from "@google/design.md/linter";

const ROOT = `${import.meta.dir}/..`;
const failures: string[] = [];

// ── 1. Token discipline ──────────────────────────────────────────────────────

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
    const bodyStart = (block.index ?? 0) + block[0].indexOf(block[1]);
    const body = stripComments(block[1]);
    for (const decl of body.matchAll(DECLARATION)) {
      const [, property, value] = decl;
      const prop = property.toLowerCase();
      const line = lineOf(source, bodyStart + (decl.index ?? 0));
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

// The rule's own fixture: it must flag literals and honour the mask exemption, or a
// regex edit could silently pass every file.
const SELF_CHECK = [
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
const selfCheckLines = tokenViolations(SELF_CHECK, "X.svelte")
  .map((v) => v.split(":")[1])
  .join(",");
if (selfCheckLines !== "3,4,6") {
  failures.push(
    `token-discipline self-check flagged lines [${selfCheckLines}], expected [3,4,6]`,
  );
}

const svelteFiles = [
  ...new Bun.Glob("src/**/*.svelte").scanSync({ cwd: ROOT }),
].sort();
if (svelteFiles.length <= 30) {
  failures.push(
    `token discipline saw only ${svelteFiles.length} .svelte files under src/`,
  );
}
for (const file of svelteFiles) {
  const source = await Bun.file(`${ROOT}/${file}`).text();
  failures.push(...tokenViolations(source, file));
}

// ── 2. DESIGN.md ↔ tokens.css parity ─────────────────────────────────────────

const designMd = await Bun.file(`${ROOT}/DESIGN.md`).text();
const tokensCss = await Bun.file(`${ROOT}/src/tokens.css`).text();

/** `{ value: 0.75, unit: "rem" }` → `"0.75rem"`, the spelling tokens.css uses. */
const dimension = (d: ResolvedDimension): string => `${d.value}${d.unit}`;

/** Every spec token this lint governs → its resolved value, spelled as CSS. */
const SPEC = ((): Map<string, string> => {
  const { colors, rounded, spacing, typography } = lint(designMd).designSystem;
  const out = new Map<string, string>();
  for (const [name, color] of colors) out.set(`colors.${name}`, color.hex);
  for (const [name, d] of rounded) out.set(`rounded.${name}`, dimension(d));
  for (const [name, d] of spacing) out.set(`spacing.${name}`, dimension(d));
  for (const [level, type] of typography) {
    if (type.fontSize)
      out.set(`typography.${level}.fontSize`, dimension(type.fontSize));
    if (type.fontWeight !== undefined) {
      out.set(`typography.${level}.fontWeight`, String(type.fontWeight));
    }
  }
  return out;
})();

/** A `:root` custom property → its declared value, comments stripped. */
const CSS: Map<string, string> = (() => {
  const css = tokensCss.replace(/\/\*[\s\S]*?\*\//g, "");
  const start = css.indexOf(":root {");
  if (start < 0) throw new Error("tokens.css has no :root block");
  // Nothing inside :root opens a brace, so the next one closes it.
  const body = css.slice(start, css.indexOf("}", start));
  const out = new Map<string, string>();
  for (const [, name, value] of body.matchAll(/--([\w-]+)\s*:\s*([^;]+);/g)) {
    out.set(`--${name}`, value.trim().replace(/\s+/g, " "));
  }
  return out;
})();

function resolveCss(property: string): string {
  const raw = CSS.get(property);
  if (raw === undefined) {
    throw new Error(`tokens.css :root declares no \`${property}\``);
  }
  return raw.replace(/var\((--[\w-]+)\)/g, (_, reference: string) =>
    resolveCss(reference),
  );
}

/** The only tokens with no custom property: they name something CSS cannot hold. */
const EXEMPT: Record<string, string> = {
  "colors.primary":
    "spec-required palette name for the ramp stop --gray-1 aliases",
  "colors.neutral":
    "spec-required palette name for the ramp stop --gray-12 aliases",
  "colors.white":
    "the white surfaces spell #ffffff out; there is no --white role",
  "spacing.rail": "the 16rem rail is an AppShell grid track, not a token",
  "spacing.content-max": "the 80rem cap is an AppShell max-width, not a token",
  "spacing.breakpoint-narrow": "a media query cannot read a custom property",
};

/** Non-color tokens whose custom property is not simply `--<name>`.
 *
 * The scale spends three weights across its eight levels, so four heading
 * levels share --heading-weight and body/body-sm/mono share --body-weight.
 * Mapping many spec tokens onto one property is what makes the property a
 * ROLE: should the spec ever weight h3 apart from h1, this lint fails until
 * tokens.css splits them. */
const PROPERTY_OF: Record<string, string> = {
  "rounded.sm": "--radius-sm",
  "rounded.md": "--radius",
  "spacing.1": "--space-1",
  "spacing.2": "--space-2",
  "spacing.3": "--space-3",
  "spacing.4": "--space-4",
  "typography.display.fontSize": "--text-display",
  "typography.h1.fontSize": "--text-h1",
  "typography.h2.fontSize": "--text-h2",
  "typography.h3.fontSize": "--text-h3",
  "typography.body.fontSize": "--text-body",
  "typography.body-sm.fontSize": "--text-sm",
  "typography.label.fontSize": "--text-micro",
  "typography.mono.fontSize": "--text-mono",
  "typography.display.fontWeight": "--heading-weight",
  "typography.h1.fontWeight": "--heading-weight",
  "typography.h2.fontWeight": "--heading-weight",
  "typography.h3.fontWeight": "--heading-weight",
  "typography.body.fontWeight": "--body-weight",
  "typography.body-sm.fontWeight": "--body-weight",
  "typography.mono.fontWeight": "--body-weight",
  "typography.label.fontWeight": "--micro-label-weight",
};

/** A color role is `--<role>` by rule; every other token is named above. */
function propertyOf(token: string): string | undefined {
  const color = token.match(/^colors\.(.+)$/);
  return PROPERTY_OF[token] ?? (color ? `--${color[1]}` : undefined);
}

if (SPEC.size <= 50) {
  failures.push(
    `DESIGN.md front matter resolved only ${SPEC.size} tokens; expected over 50`,
  );
}
for (const [token, specValue] of SPEC) {
  if (token in EXEMPT) continue;
  const property = propertyOf(token);
  if (!property) {
    failures.push(
      `DESIGN.md token \`${token}\` has no tokens.css property (map it or exempt it)`,
    );
    continue;
  }
  const cssValue = resolveCss(property).toLowerCase();
  if (cssValue !== specValue.toLowerCase()) {
    failures.push(
      `DESIGN.md \`${token}\` is ${specValue}, but tokens.css \`${property}\` resolves to ${cssValue}`,
    );
  }
}

if (failures.length > 0) {
  for (const failure of failures) console.error(failure);
  console.error(`lint:tokens: ${failures.length} problem(s)`);
  process.exit(1);
}
console.log(
  `lint:tokens: ${svelteFiles.length} .svelte files and ${SPEC.size} DESIGN.md tokens clean`,
);
