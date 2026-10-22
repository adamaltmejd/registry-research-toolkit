// Design-token source lints, run by `bun run lint:tokens` (part of `bun run lint`).
// Deterministic source checks, not behavior tests: no browser, no LLM.
//
// 1. Token discipline. DESIGN.md → "Visual language" → "Token architecture":
//    components consume SEMANTIC roles only. A raw color literal or a raw font stack
//    inside a component's <style> block renders identically today and still breaks
//    the light-first, dark-ready contract (the [data-theme="dark"] remap cannot reach
//    it). Screenshots cannot catch that bypass and biome does not lint <style> blocks
//    inside .svelte, so this scans every .svelte <style> block, every stylesheet
//    under src/ but tokens.css (CSS Modules included) and every `style={{…}}` object
//    in a .tsx for a literal that should be a var(--token). Exempt by rule, not by allowlist: mask-image declarations (a
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
//
// 3. Reference resolution. A `var(--name)` whose name no stylesheet defines renders
//    the fallback (or the property's initial value) without a warning, so a typo like
//    `var(--txet)` ships silently. Every reference in a .svelte, .css or .tsx file under
//    src/ must name a custom property tokens.css declares or the same file declares (in
//    a <style> block, an inline `style="--x: …"`, a `style:--x` directive, or a
//    `"--x":` key of a React style object). A property
//    set in one file and read in another is named in CROSS_FILE with its reason.
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

// A React style object, `style={{ … }}`, and one of its entries: a bare, quoted or
// computed key, then a quoted value or one up to the next comma or brace.
const STYLE_OBJECT = /style=\{\{([\s\S]*?)\}\}/g;
const OBJECT_ENTRY =
  /([\w-]+|"[^"]*"|'[^']*')\s*:\s*("[^"]*"|'[^']*'|`[^`]*`|[^,}]+)/g;

/** The style bodies of one file, each with its offset into `source`: a .svelte's
 * <style> blocks, a stylesheet whole, a .tsx's style objects. */
function styleBodies(
  source: string,
  file: string,
): { body: string; start: number; entry: RegExp }[] {
  if (file.endsWith(".css")) {
    return [{ body: source, start: 0, entry: DECLARATION }];
  }
  const [blocks, entry] = file.endsWith(".tsx")
    ? [STYLE_OBJECT, OBJECT_ENTRY]
    : [STYLE_BLOCK, DECLARATION];
  return [...source.matchAll(blocks)].map((block) => ({
    body: block[1],
    start: (block.index ?? 0) + block[0].indexOf(block[1]),
    entry,
  }));
}

function tokenViolations(source: string, file: string): string[] {
  const out: string[] = [];
  for (const { body: raw, start: bodyStart, entry } of styleBodies(
    source,
    file,
  )) {
    const body = stripComments(raw);
    for (const decl of body.matchAll(entry)) {
      const [, property] = decl;
      const value = decl[2].replace(/^["'`]|["'`]$/g, "");
      // A React key is camelCase (`fontFamily`); compare in CSS spelling.
      const prop = property
        .replace(/^["']|["']$/g, "")
        .replace(/[A-Z]/g, (c) => `-${c}`)
        .toLowerCase();
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
// The same rule over a React style object and a CSS Module.
const TSX_SELF_CHECK = [
  "<div style={{ color: '#fff', width: 1 }} />",
  "<div style={{ fontFamily: 'ui-monospace, monospace' }} />",
  '<div style={{ "--tree-indent": "var(--space-5)", color: "var(--text)" }} />',
].join("\n");
const CSS_SELF_CHECK =
  ".a { color: var(--text); }\n.b { color: hsl(0 0% 0%); }";
const flaggedLines = (source: string, file: string): string =>
  tokenViolations(source, file)
    .map((v) => v.split(":")[1])
    .join(",");
if (flaggedLines(TSX_SELF_CHECK, "X.tsx") !== "1,2") {
  failures.push(
    `token-discipline .tsx self-check flagged lines [${flaggedLines(TSX_SELF_CHECK, "X.tsx")}], expected [1,2]`,
  );
}
if (flaggedLines(CSS_SELF_CHECK, "X.module.css") !== "2") {
  failures.push(
    `token-discipline .css self-check flagged lines [${flaggedLines(CSS_SELF_CHECK, "X.module.css")}], expected [2]`,
  );
}

// tokens.css is the one file whose job is to hold the literals.
const disciplinedFiles = [
  ...new Bun.Glob("src/**/*.{svelte,css,tsx}").scanSync({ cwd: ROOT }),
]
  .filter((file) => file !== "src/tokens.css")
  .sort();
const svelteFiles = disciplinedFiles.filter((f) => f.endsWith(".svelte"));
if (svelteFiles.length <= 30) {
  failures.push(
    `token discipline saw only ${svelteFiles.length} .svelte files under src/`,
  );
}
for (const file of disciplinedFiles) {
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
  "colors.primary": "spec-required palette name for the accent role it aliases",
  "colors.neutral":
    "spec-required palette name for the ramp stop --gray-12 aliases",
  "colors.white":
    "the white surfaces spell #ffffff out; there is no --white role",
  "spacing.rail": "the 16rem rail is an AppShell grid track, not a token",
  "spacing.breakpoint-narrow": "a media query cannot read a custom property",
};

/** Non-color tokens whose custom property is not simply `--<name>`.
 *
 * The scale spends four weights across its ten levels: display and h1 share
 * --display-weight, h2 and h3 share --heading-weight, body/body-sm/mono/axis
 * share --body-weight, and the label and column levels have their own.
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
  "typography.column.fontSize": "--text-column",
  "typography.axis.fontSize": "--text-axis",
  "spacing.5": "--space-5",
  "spacing.6": "--space-6",
  "spacing.col-browse": "--col-browse",
  "spacing.col-list": "--col-list",
  "spacing.lane-height": "--lane-height",
  "spacing.content-max": "--content-max",
  "typography.display.fontWeight": "--display-weight",
  "typography.h1.fontWeight": "--display-weight",
  "typography.h2.fontWeight": "--heading-weight",
  "typography.h3.fontWeight": "--heading-weight",
  "typography.body.fontWeight": "--body-weight",
  "typography.body-sm.fontWeight": "--body-weight",
  "typography.mono.fontWeight": "--body-weight",
  "typography.label.fontWeight": "--micro-label-weight",
  "typography.column.fontWeight": "--column-weight",
  "typography.axis.fontWeight": "--body-weight",
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

// ── 3. Reference resolution ──────────────────────────────────────────────────

/** Custom properties written in one file and read in another, each with its writer. */
const CROSS_FILE: Record<string, string> = {
  "--facet-axis-hue":
    "set inline by catalog.ts facetAxisStyle(), read by RepresentationPicker",
  "--facet-axis-ink":
    "set inline by catalog.ts facetAxisStyle(), read by RepresentationPicker",
  "--track-width":
    "DualThumbTrack sizing hook with a fallback; consumers may override it",
  "--track-height":
    "DualThumbTrack sizing hook, overridden by PeriodWindowSlider",
  "--rail-height":
    "DualThumbTrack sizing hook, overridden by PeriodWindowSlider",
  "--thumb-size":
    "DualThumbTrack sizing hook, overridden by PeriodWindowSlider",
  "--tree-item-level":
    "set inline by React Aria's Tree on each row, read by BrowseTree",
  "--top-bar-height":
    "measured by TopBar onto :root, read by the shell's browse column",
  "--data-table-columns":
    "set inline by DataTable.tsx on its table, read by DataTable.module.css",
  "--code-depth":
    "set inline by CodeList.tsx on each levelled row, read by CodeList.module.css",
  "--cue-left":
    "set inline by PeriodPicker.tsx on the coverage cue, read by PeriodPicker.module.css",
};

const VAR_REFERENCE = /var\(\s*(--[\w-]+)/g;
// `--x:` in CSS or an inline style attribute, a Svelte `style:--x` directive, or a
// `"--x":` key in a React style object.
const DECLARED = /(--[\w-]+)["']?\s*[:=]/g;
const HTML_COMMENT = /<!--[\s\S]*?-->/g;

const declaredIn = (text: string): Set<string> =>
  new Set([...text.matchAll(DECLARED)].map((m) => m[1]));
const TOKEN_PROPERTIES = declaredIn(stripComments(tokensCss));

function unresolvedReferences(source: string, file: string): string[] {
  const text = stripComments(source).replace(HTML_COMMENT, (m) =>
    m.replace(/[^\n]/g, " "),
  );
  const local = declaredIn(text);
  const out: string[] = [];
  for (const ref of text.matchAll(VAR_REFERENCE)) {
    const name = ref[1];
    if (TOKEN_PROPERTIES.has(name) || local.has(name) || name in CROSS_FILE)
      continue;
    out.push(
      `${file}:${lineOf(text, ref.index ?? 0)}: var(${name}) names no custom property in tokens.css or this file`,
    );
  }
  return out;
}

const REFERENCE_SELF_CHECK = [
  "<div style:--local-a={1} style='--local-b: 2'></div>",
  "<style>",
  "  .a { color: var(--text); width: var(--local-a); height: var(--local-b); }",
  "  .b { color: var(--txet); }",
  "  .c { --local-c: 1px; margin: var(--local-c); }",
  "  .d { padding: var(--thumb-size, 1rem) var(--nope, 0); }",
  "  /* var(--in-a-comment) */",
  "</style>",
].join("\n");
const referenceSelfCheck = unresolvedReferences(
  REFERENCE_SELF_CHECK,
  "X.svelte",
)
  .map((v) => v.split(":")[1])
  .join(",");
if (referenceSelfCheck !== "4,6") {
  failures.push(
    `reference-resolution self-check flagged lines [${referenceSelfCheck}], expected [4,6]`,
  );
}

const styledFiles = [
  ...new Bun.Glob("src/**/*.{svelte,css,tsx}").scanSync({ cwd: ROOT }),
].sort();
for (const file of styledFiles) {
  const source = await Bun.file(`${ROOT}/${file}`).text();
  failures.push(...unresolvedReferences(source, file));
}

if (failures.length > 0) {
  for (const failure of failures) console.error(failure);
  console.error(`lint:tokens: ${failures.length} problem(s)`);
  process.exit(1);
}
console.log(
  `lint:tokens: ${disciplinedFiles.length} styled files, ${SPEC.size} DESIGN.md tokens and var() references in ${styledFiles.length} files clean`,
);
