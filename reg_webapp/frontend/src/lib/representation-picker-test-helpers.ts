// Shared fixtures + DOM helpers for the RepresentationPicker.*.browser.test.ts split
// (graph, graph-fallback, graph-history, labels, filters, staging, row-identity).
// Browser-only: imports `vitest/browser`.
import { vi } from "vitest";
import { page } from "vitest/browser";
import type {
  GraphEdge,
  GraphState,
  GroupAxisModel,
  RelationshipGraph,
  VariableGraphNode,
} from "./api";
import type { PickerRepresentation } from "./catalog";
import type { PickerBand } from "./RepresentationPicker.svelte";

export function row(over: Partial<PickerRepresentation>): PickerRepresentation {
  return {
    key: `${over.variant ?? "v"}::${over.column ?? "Col"}`,
    variant: "v",
    variantLabel: over.variant ?? "v",
    column: over.column ?? "Col",
    representation: over.column ?? "Col",
    from: "2000-01-01",
    to: "2010-12-31",
    windows: [{ from: "2000-01-01", to: "2010-12-31" }],
    period: "2000 – 2010",
    wirePeriod: "2000..2010",
    valueSetLabel: "",
    codingsVary: false,
    renamedColumns: [],
    ...over,
  };
}

export function graphState(over: Partial<GraphState> = {}): GraphState {
  return {
    state_id: "1",
    period_scope: "intervals",
    variant: "v",
    variant_label: null,
    variant_family: null,
    variant_family_label: null,
    representation_run_id: 1,
    valid_from: "2000-01-01",
    valid_to: "2010-12-31",
    value_set_id: null,
    value_set_version_label: "",
    classification_slugs: [],
    delivery_column_name: "Col",
    ...over,
  };
}

export function graphNode(
  fqid: string,
  over: Partial<VariableGraphNode> = {},
): VariableGraphNode {
  return {
    kind: "variable",
    id: fqid,
    fqid,
    label: fqid.split("/").at(-1) ?? fqid,
    group_key: "group",
    group_label: "Concept group",
    definition: null,
    description: null,
    operational_definition: null,
    facets: [],
    states: [graphState({ delivery_column_name: "Col" })],
    same_as: [],
    ...over,
  };
}

export function graph(
  over: Partial<RelationshipGraph> = {},
): RelationshipGraph {
  return { nodes: [], edges: [], focus_id: null, ...over };
}

export function edge(
  source: string,
  target: string,
  over: Partial<GraphEdge> = {},
): GraphEdge {
  return {
    id: `${source}->${target}`,
    kind: "succession",
    source,
    target,
    label: null,
    ...over,
  };
}

/** A multi-axis representation group: one band, three delivery-column rows, each a
 * member with a distinct (enhet, hush) facet pair — the shape the #819 families
 * have and that #908's filters narrow. */
export function multiAxisBand(): PickerBand {
  return {
    key: "scb/iot/dispink",
    name: "Disponibel inkomst",
    registerPrefix: "scb/iot",
    rows: [
      row({ column: "DIN1", valueSetLabel: "kr" }),
      row({ column: "DIN2", valueSetLabel: "kr" }),
      row({ column: "DIN3", valueSetLabel: "kr" }),
    ],
    facetsByColumn: {
      DIN1: [
        { axis: "enhet", value: "ind", label: "Individ" },
        { axis: "hush", value: "h1", label: "Hushall" },
      ],
      DIN2: [
        { axis: "enhet", value: "ind", label: "Individ" },
        { axis: "hush", value: "h2", label: "Familj" },
      ],
      DIN3: [
        { axis: "enhet", value: "fam", label: "Konsumtionsenhet" },
        { axis: "hush", value: "h1", label: "Hushall" },
      ],
    },
  };
}

export const AXES: GroupAxisModel[] = [
  { name: "enhet", label: "Enhet" },
  { name: "hush", label: "Hushallsbegrepp" },
];
export const PROPS = {
  window: null,
  canAdd: true,
  onapply: vi.fn(),
} as const;

/** The delivery-column chips of the currently-visible column ROWS (not the filter
 * fieldsets). */
export function visibleColumns(): (string | undefined)[] {
  return [...document.querySelectorAll(".col-list .col-row .col-chip")].map(
    (c) => c.textContent?.replace("↗", "").trim(),
  );
}

/** Click a filter CHIP (a `ui/FilterChip` labelled checkbox inside a `.dim-filter`
 * fieldset) by its value text — scoped to `.dim-filters` so it never hits a row
 * checkbox. */
export function clickFilter(value: string): void {
  const chip = [...document.querySelectorAll(".dim-filters .ui-chip")].find(
    (c) => c.textContent?.trim() === value,
  ) as HTMLElement | undefined;
  if (!chip) {
    throw new Error(`filter chip not found: ${value}`);
  }
  chip.click();
}

/** The one element matching `sel`, or a failure naming it — a missing node must fail
 * loudly rather than pass every geometry assertion below as a 0. */
export function one<T extends Element>(
  sel: string,
  within: ParentNode = document,
): T {
  const found = within.querySelector<T>(sel);
  if (!found) {
    throw new Error(`missing ${sel}`);
  }
  return found;
}

export function lanes(): HTMLElement[] {
  return [...document.querySelectorAll<HTMLElement>(".graph-lane")];
}

/** Run `check` at each width the design language covers, restoring the viewport
 * afterwards so a failure cannot leak one into the specs that follow. */
export async function atEveryWidth(
  check: (width: number, height: number) => Promise<void>,
): Promise<void> {
  const viewport = { width: window.innerWidth, height: window.innerHeight };
  try {
    for (const [width, height] of [
      [375, 812],
      [768, 1024],
      [1280, 900],
      [1920, 1080],
    ]) {
      await page.viewport(width, height);
      await check(width, height);
    }
  } finally {
    await page.viewport(viewport.width, viewport.height);
  }
}
