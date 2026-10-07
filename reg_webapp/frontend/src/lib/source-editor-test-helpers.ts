// Shared fixtures for the SourceEditor browser tests (SourceEditor.browser.test.ts,
// SourceEditor.period.browser.test.ts): catalog stubs, the card render and a seeded
// source. Browser-only: renders a component; import only from *.browser.test.ts.
import type { ComponentProps } from "svelte";
import { vi } from "vitest";
import { render } from "vitest-browser-svelte";
import type {
  CatalogNode,
  RootResponse,
  StatesResponse,
  VariantsResponse,
} from "./api";
import { getCatalogNode, getCatalogRoot, getRegisterVariants } from "./api";
import type { Period, Source } from "./project_data";
import { projectStore } from "./project_store.svelte";
import SourceEditor from "./SourceEditor.svelte";

/** The catalog root as a deployment serving `providers` (the shell's facet list,
 * and what decides whether a card's title carries its provider). */
export function rootResponse(
  ...providers: { fqid: string; name: string }[]
): RootResponse {
  return {
    kind: "root",
    children: providers.map(({ fqid, name }) => ({
      kind: "provider",
      fqid,
      name,
    })),
  } as unknown as RootResponse;
}

/** A register entry carrying its curated display name — "LISA", "MiDAS": the word
 * the register's own catalog page is headed with. */
export function registerNode(fqid: string, name: string | null) {
  return { kind: "register", fqid, name, purpose: null, coverage: null };
}

/** A provider node listing the registers it owns — the read the card's register
 * word comes from (one light payload per provider, not one register node each). */
export function providerNode(
  fqid: string,
  ...registers: ReturnType<typeof registerNode>[]
): CatalogNode {
  return {
    kind: "provider",
    fqid,
    name: fqid,
    children: registers,
  } as unknown as CatalogNode;
}

/** A register's variant list, as `GET /{provider}/{register}/variants` returns it. */
export function variantsResponse(
  ...variants: { slug: string; name?: string | null }[]
): VariantsResponse {
  return { variants } as unknown as VariantsResponse;
}

/** The LISA fixture every case starts from: a single-provider deployment whose
 * `scb/lisa` register delivers the two individual-frame variants of one succession
 * family plus the workplace frame. A case that needs another register/deployment
 * overrides the mock it cares about. */
export function stubCatalog(): void {
  vi.mocked(getCatalogRoot).mockResolvedValue(
    rootResponse({ fqid: "scb", name: "Statistiska Centralbyrån" }),
  );
  vi.mocked(getCatalogNode).mockImplementation(async (fqid) =>
    fqid === "scb"
      ? providerNode("scb", registerNode("scb/lisa", "LISA"))
      : // The columns' own leaf resolve (BindingEditor.browser.test.ts owns it):
        // nothing covering, so the rows show their FQID alone.
        ({ states: [] } as unknown as StatesResponse),
  );
  vi.mocked(getRegisterVariants).mockResolvedValue(
    variantsResponse(
      { slug: "v1", name: "Individer 15+" },
      { slug: "individer-15plus", name: "Individer, 15 år och äldre" },
      { slug: "individer-16plus", name: "Individer, 16 år och äldre" },
      { slug: "arbetsstallen", name: "Arbetsställen" },
    ),
  );
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
