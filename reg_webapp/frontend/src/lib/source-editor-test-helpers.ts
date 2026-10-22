// Shared fixtures for the SourceEditor browser tests (SourceEditor.browser.test.ts,
// SourceEditor.period.browser.test.ts): catalog stubs, the card render and a seeded
// source. Browser-only: renders a component; import only from *.browser.test.ts.
import type { ComponentProps } from "svelte";
import { render } from "vitest-browser-svelte";
import type {
  ProviderShow,
  RegisterChild,
  RegisterShow,
  ShowNode,
  VariableStateModel,
} from "./api";
import { registerShow, rootShow, stubApi } from "./api-test-helpers";
import { storedProject } from "./project-store-test-helpers";
import type { ProjectSource, ProjectSourcePeriod } from "./project_data";
import { projectStore } from "./project_store.svelte";
import SourceEditor from "./SourceEditor.svelte";
import { variant } from "./variants-test-helpers";

/** A provider's register entry carrying its curated display name — "LISA",
 * "MiDAS": the word the register's own catalog page is headed with. */
export function registerChild(
  fqid: string,
  name: string | null,
): RegisterChild {
  return { fqid, name, purpose: null, tags: [], coverage: null };
}

/** A provider's `show`, listing the registers it owns — the read the card's
 * register word comes from (one light payload per provider, not one register
 * node each). */
export function providerShow(
  fqid: string,
  ...registers: RegisterChild[]
): ProviderShow {
  return { kind: "provider", fqid, name: fqid, children: registers };
}

/** A register's `show`, carrying its variants (the card's variant word). */
export function variantsRegister(
  fqid: string,
  ...variants: { slug: string; name?: string | null }[]
): RegisterShow {
  return registerShow({
    fqid,
    name: null,
    variants: variants.map(({ slug, name }) =>
      variant(slug, { name: name ?? null }),
    ),
  });
}

/** The LISA catalog every case starts from: a single-provider deployment whose
 * `scb/lisa` register delivers the two individual-frame variants of one
 * succession family plus the workplace frame. Keyed by ref, `""` the root. */
function lisaCatalog(): Record<string, ShowNode | Error> {
  return {
    "": rootShow({ fqid: "scb", name: "Statistiska Centralbyrån" }),
    scb: providerShow("scb", registerChild("scb/lisa", "LISA")),
    "scb/lisa": variantsRegister(
      "scb/lisa",
      { slug: "v1", name: "Individer 15+" },
      { slug: "individer-15plus", name: "Individer, 15 år och äldre" },
      { slug: "individer-16plus", name: "Individer, 16 år och äldre" },
      { slug: "arbetsstallen", name: "Arbetsställen" },
    ),
  };
}

/** Serve `show` over the LISA catalog with `overrides` laid over it by ref (an
 * `Error` makes that read fail; a ref in neither is a 404), the columns' own
 * `states` read (BindingEditor.browser.test.ts owns it) from `states` — by
 * default nothing covering, so the rows show their FQID alone — and no data
 * warnings. Returns the fetch spy. */
export function stubCatalog(
  overrides: Record<string, ShowNode | Error> = {},
  states: (
    fqid: string,
    query: URLSearchParams,
  ) => VariableStateModel[] = () => [],
) {
  const catalog = { ...lisaCatalog(), ...overrides };
  return stubApi((url) => {
    const [, , read, ...ref] = url.pathname.split("/");
    const fqid = ref.map(decodeURIComponent).join("/");
    if (read === "states") {
      return { items: states(fqid, url.searchParams), next_cursor: null };
    }
    if (read === "warnings") {
      return [];
    }
    if (read !== "catalog") {
      return undefined;
    }
    const node = catalog[fqid];
    if (node instanceof Error) {
      throw node;
    }
    return node;
  });
}

/** The `show` reads `spy` sent for `ref`. */
export function showReads(
  spy: ReturnType<typeof stubCatalog>,
  ref: string,
): number {
  return spy.mock.calls.filter(
    ([input]) =>
      new URL(String(input), "http://localhost").pathname ===
      `/api/catalog/${ref}`,
  ).length;
}

/** The card under test, always at index 0 of the fresh draft above. Every case
 * renders it with the same three constants, so a case that overrides one — the
 * multi-provider deployment, a standing validation error — says so and nothing
 * else. */
export function renderCard(
  source: ProjectSource,
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
export function seedSource(period: ProjectSourcePeriod): ProjectSource {
  projectStore.applyStagedDiff({
    adds: [
      {
        registerVariant: "scb/lisa/arbetsstallen",
        period,
        binding: { variable: "scb/lisa/kon", type: "categorical" },
      },
    ],
  });
  return storedProject()?.sources?.[0] as ProjectSource;
}
