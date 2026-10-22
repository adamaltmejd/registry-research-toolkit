// Typed builders for the API wire shapes the unit and browser suites hand-write: one
// spelling of each, typed by the codegen'd `./api` types with no cast, so a wire
// change fails HERE rather than passing through every suite. Each suite keeps its
// own defaults as a thin wrapper (`state`, `vstate`, `columnState`). The builders
// are pure; `stubApi` stubs the HTTP boundary those shapes cross. Import from any
// test.
import { onTestFinished, vi } from "vite-plus/test";
import type { RegisterShow, RootShow, VariableStateModel } from "./api";
import { variant } from "./variants-test-helpers";

/** Stub `fetch` at the HTTP boundary, the one process boundary the SPA's reads
 * cross, instead of mocking `./api`: the request URL, query and envelope are then
 * the real ones. `route` gets each request as a URL (pathname under `/api`, its
 * search params) and returns what the Rust server would answer — the `data` of its
 * `{data, meta}` envelope, or a whole `Response` (an error status). Returning
 * `undefined` answers 404 `not_found`, so a request a case did not expect fails
 * where it is made; throwing fails the request as a network error would. Returns
 * the spy, whose calls are the requests sent. Unstubbed when the test ends. */
export function stubApi(
  route: (url: URL) => unknown,
): ReturnType<typeof vi.fn<typeof fetch>> {
  const spy = vi.fn<typeof fetch>(async (input) => {
    const url = new URL(String(input), "http://localhost");
    const answer = await route(url);
    if (answer instanceof Response) {
      return answer;
    }
    if (answer === undefined) {
      return apiError(404, "not_found", `no route for ${url.pathname}`);
    }
    return Response.json({ data: answer, meta: {} });
  });
  vi.stubGlobal("fetch", spy);
  onTestFinished(() => {
    vi.unstubAllGlobals();
  });
  return spy;
}

/** An error response in the Rust server's envelope. */
export function apiError(
  status: number,
  code: string,
  message: string,
): Response {
  return Response.json({ error: { code, message }, meta: {} }, { status });
}

/** A `VariableStateModel` with neutral defaults; `over` sets what a case reads. */
export function variableState(
  over: Partial<VariableStateModel> = {},
): VariableStateModel {
  return {
    warning_ids: [],
    state_id: "1",
    period_scope: "intervals",
    period_token: null,
    variant: "v",
    variant_label: null,
    variant_family: null,
    variant_family_label: null,
    register_variant_id: "1",
    valid_from: "",
    valid_to: "",
    coding_window_from: null,
    name: null,
    definition: null,
    description: null,
    operational_definition: null,
    measurement_unit: null,
    data_type: null,
    data_length: null,
    delivery_column_name: null,
    source_register_text: null,
    provenance: null,
    pooled: false,
    value_set_version_label: "",
    value_set_id: null,
    value_set: null,
    value_set_summary: null,
    is_identifier: false,
    classifications: [],
    ...over,
  };
}

/** The catalog root of a deployment serving `providers` (the shell's facet list,
 * and what decides whether a source card's title carries its provider). */
export function rootShow(
  ...providers: { fqid: string; name: string }[]
): RootShow {
  return {
    kind: "root",
    catalog: {
      id: "global",
      reg_meta_version: "0.0.0",
      project_schema_version: "3.0.0",
    },
    children: providers.map(({ fqid, name }) => ({
      kind: "provider" as const,
      fqid,
      name,
    })),
  };
}

/** A register `show` node (`scb/lisa`, "LISA"); `fields` overrides any part of it.
 * Its one variant is `_default` unless `fields.variants` names others: the register
 * page reads its variants off the node itself. */
export function registerShow(
  fields: Partial<Omit<RegisterShow, "kind">> = {},
): RegisterShow {
  return {
    kind: "register",
    fqid: "scb/lisa",
    name: "LISA",
    purpose: null,
    tags: [],
    groups: [],
    variants: [variant("_default")],
    children: [],
    ...fields,
  };
}
