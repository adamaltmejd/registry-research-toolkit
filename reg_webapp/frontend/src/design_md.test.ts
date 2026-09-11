// Parity between the design language and its CSS implementation.
//
// `reg_webapp/frontend/DESIGN.md` is the normative design language (the DESIGN.md
// format: the YAML front matter IS the token set). `bun run lint:design` checks
// that file's own shape; this test checks the other half — that `src/tokens.css`
// actually RESOLVES to those values, so a color, radius, spacing step or type
// size can only move by moving the spec first.
//
// The spec side is read through the same pinned `@google/design.md` the lint
// script runs: `lint()` hands back the front matter already resolved — `{a.b}`
// references followed, colors normalized to hex — so this file only has to
// resolve the OTHER side, the `var(--x)` chains in the `:root` block.
import { lint, type ResolvedDimension } from "@google/design.md/linter";
import { describe, expect, it } from "vitest";
import designMd from "../DESIGN.md?raw";
import tagSvelte from "./lib/ui/Tag.svelte?raw";
import tokensCss from "./tokens.css?raw";

/** `{ value: 0.75, unit: "rem" }` → `"0.75rem"`, the spelling tokens.css uses. */
const dimension = (d: ResolvedDimension): string => `${d.value}${d.unit}`;

/** Every spec token this test governs → its resolved value, spelled as CSS. */
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
 * ROLE: should the spec ever weight h3 apart from h1, this test fails until
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

describe("DESIGN.md ↔ tokens.css parity", () => {
  it("governs every color, radius, spacing step and type size in the spec", () => {
    expect(SPEC.size).toBeGreaterThan(50); // the front matter must have resolved
    const unaccounted = [...SPEC.keys()].filter(
      (token) => !(token in EXEMPT) && propertyOf(token) === undefined,
    );
    expect(unaccounted).toEqual([]);
  });

  it("resolves each token to the same value on both sides", () => {
    const actual: Record<string, string> = {};
    const expected: Record<string, string> = {};
    for (const [token, specValue] of SPEC) {
      const property = token in EXEMPT ? undefined : propertyOf(token);
      if (!property) continue;
      actual[`${token} (${property})`] = resolveCss(property).toLowerCase();
      expected[`${token} (${property})`] = specValue.toLowerCase();
    }
    expect(actual).toEqual(expected);
  });

  it("binds the tag primitive's face to Tag.svelte's own base rule", () => {
    // components.tag.typography resolves through the spec's own reference (it
    // names a font family, which SPEC above does not track), so this checks the
    // one component binding the ticket added rather than reusing SPEC/CSS.
    const FONT_FAMILY_VAR: Record<string, string> = {
      "Schibsted Grotesk": "--font-ui",
      "IBM Plex Mono": "--font-mono",
    };
    const typography = lint(designMd)
      .designSystem.components.get("tag")
      ?.properties.get("typography");
    if (
      !typography ||
      typeof typography !== "object" ||
      typography.type !== "typography"
    ) {
      throw new Error("components.tag.typography did not resolve");
    }
    const expectedVar = FONT_FAMILY_VAR[typography.fontFamily ?? ""];
    expect(expectedVar).toBeDefined();

    const baseRule = tagSvelte.match(/\.tag\s*{[^}]*}/)?.[0];
    if (!baseRule) throw new Error("Tag.svelte has no base .tag rule");
    const actualVar = baseRule.match(/font-family:\s*var\((--[\w-]+)\)/)?.[1];
    expect(actualVar).toBe(expectedVar);
  });
});
