// Shared fixtures for the SourceEditor browser tests (SourceEditor.browser.test.ts,
// SourceEditor.period.browser.test.ts): catalog stubs, the card render and a seeded
// source. Browser-only: renders a component; import only from *.browser.test.ts.
import type { ComponentProps } from "svelte";
import { vi } from "vitest";
import { render } from "vitest-browser-svelte";
import { getShow, getStates, type ShowNode } from "./api";
import type { Period, Source } from "./project_data";
import { projectStore } from "./project_store.svelte";
import SourceEditor from "./SourceEditor.svelte";

/** The catalog root as a deployment serving `providers` (the shell's facet list,
 * and what decides whether a card's title carries its provider). */
export function rootShow(
  ...providers: { fqid: string; name: string }[]
): ShowNode {
  return {
    kind: "root",
    children: providers.map(({ fqid, name }) => ({
      kind: "provider" as const,
      fqid,
      name,
    })),
  } as unknown as ShowNode;
}

/** A provider's register entry carrying its curated display name — "LISA",
 * "MiDAS": the word the register's own catalog page is headed with. */
export function registerChild(fqid: string, name: string | null) {
  return { fqid, name, purpose: null, tags: [], coverage: null };
}

/** A provider's `show`, listing the registers it owns — the read the card's
 * register word comes from (one light payload per provider, not one register
 * node each). */
export function providerShow(
  fqid: string,
  ...registers: ReturnType<typeof registerChild>[]
): ShowNode {
  return {
    kind: "provider",
    fqid,
    name: fqid,
    children: registers,
  } as unknown as ShowNode;
}

/** A register's `show`, carrying its variants (the card's variant word). */
export function registerShow(
  fqid: string,
  ...variants: { slug: string; name?: string | null }[]
): ShowNode {
  return {
    kind: "register",
    fqid,
    name: null,
    children: [],
    groups: [],
    tags: [],
    variants: variants.map((v) => ({ versions: [], ...v })),
  } as unknown as ShowNode;
}

/** The LISA catalog every case starts from: a single-provider deployment whose
 * `scb/lisa` register delivers the two individual-frame variants of one
 * succession family plus the workplace frame. Keyed by ref, `""` the root. */
function lisaCatalog(): Record<string, ShowNode | Error> {
  return {
    "": rootShow({ fqid: "scb", name: "Statistiska Centralbyrån" }),
    scb: providerShow("scb", registerChild("scb/lisa", "LISA")),
    "scb/lisa": registerShow(
      "scb/lisa",
      { slug: "v1", name: "Individer 15+" },
      { slug: "individer-15plus", name: "Individer, 15 år och äldre" },
      { slug: "individer-16plus", name: "Individer, 16 år och äldre" },
      { slug: "arbetsstallen", name: "Arbetsställen" },
    ),
  };
}

/** Stub `show` over the LISA catalog with `overrides` laid over it by ref (an
 * `Error` makes that read fail; a ref in neither is a 404-like failure), and the
 * columns' own `states` read (BindingEditor.browser.test.ts owns it) to nothing
 * covering, so the rows show their FQID alone. */
export function stubCatalog(
  overrides: Record<string, ShowNode | Error> = {},
): void {
  const catalog = { ...lisaCatalog(), ...overrides };
  vi.mocked(getShow).mockImplementation(async (ref) => {
    const node = catalog[ref ?? ""];
    if (node === undefined) throw new Error(`no catalog node ${ref}`);
    if (node instanceof Error) throw node;
    return node;
  });
  vi.mocked(getStates).mockResolvedValue([]);
}

/** The card under test, always at index 0 of the fresh draft above. Every case
 * renders it with the same three constants, so a case that overrides one — the
 * multi-provider deployment, a standing validation error — says so and nothing
 * else. */
export function renderCard(
  source: Source,
  overrides: Partial<ComponentProps<typeof SourceEditor>> = {},
) {
  return render(SourceEditor, {
    sourceIndex: 0,
    source,
    issues: [],
    providerQualified: false,
    studyWindow: null,
    ...overrides,
  });
}

/** A one-column LISA Arbetsställen source IN THE DRAFT — the store's staleness
 * guard re-reads the draft, so a card edited against a detached object would be
 * refused every time. Returns the slot to render. */
export function seedSource(period: Period): Source {
  projectStore.applyStagedDiff({
    adds: [
      {
        registerVariant: "scb/lisa/arbetsstallen",
        period,
        binding: { variable: "scb/lisa/kon", type: "categorical" },
      },
    ],
  });
  return projectStore.draft?.sources?.[0] as Source;
}
