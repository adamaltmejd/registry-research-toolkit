<script lang="ts">
  import { onMount } from "svelte";

  import {
    getColumnValues,
    type ColumnValuesResponse,
    type ValueSetGroup,
  } from "../api";
  import Modal from "./Modal.svelte";
  import Picker from "./Picker.svelte";

  interface Props {
    register: string | null;
    column: string;
    /** Project sources in the calling group with their detected year.
     * Used to render "applies to: foo_2018.csv (2018), …" against the
     * value-set picker so the user sees which of *their* sources a
     * given year window covers — regmeta-level cvids are noisy and not
     * project-specific. Empty = no source info available (the chip
     * just falls back to the year range). */
    sourceYears?: Record<string, number | null>;
    onClose: () => void;
  }

  let { register, column, sourceYears = {}, onClose }: Props = $props();

  // Loading + result state. The fetch is on-demand (per CLAUDE.md
  // request: "fetching them on click from regmeta, not prefetching"), so
  // the modal opens immediately and replaces "Loading…" with the result
  // once the network round-trip completes. Errors render inline rather
  // than as a toast — the popover is throwaway, so co-locating the
  // failure with the empty list saves the user a glance away.
  type LoadState =
    | { kind: "loading" }
    | { kind: "ok"; data: ColumnValuesResponse }
    | { kind: "error"; message: string };
  // `null` = render the column's default (most-common) classification.
  // Set by clicking a chip in the picker; passed back to the server on
  // re-fetch so the popup honors the pick.
  let pickedClassification: string | null = $state(null);
  // null = let the server decide (union for tier 2, most-common for
  // tier 3a). A sentinel `"union"` chip in the tier-2 picker maps back
  // to null so the user can return to the default after drilling in.
  let pickedValueSet: number | null = $state(null);
  let loadState: LoadState = $state({ kind: "loading" });

  onMount(() => {
    void load();
  });

  // Project years (deduped, non-null) drive the server-side filter so
  // the picker only surfaces value-sets that intersect the user's data.
  // Yearless project sources can't be placed on the timeline and don't
  // participate in the filter; the popup still tells the user about
  // them in the "applies to" line.
  let relevantYears = $derived(
    Array.from(
      new Set(
        Object.values(sourceYears).filter(
          (y): y is number => typeof y === "number",
        ),
      ),
    ).sort((a, b) => a - b),
  );

  async function load(): Promise<void> {
    loadState = { kind: "loading" };
    try {
      const data = await getColumnValues({
        register,
        column,
        picked_classification: pickedClassification,
        picked_value_set: pickedValueSet,
        relevant_years: relevantYears,
      });
      loadState = { kind: "ok", data };
    } catch (exc) {
      const message = exc instanceof Error ? exc.message : String(exc);
      loadState = { kind: "error", message };
    }
  }

  function pickClassification(short_name: string): void {
    if (pickedClassification === short_name) return;
    pickedClassification = short_name;
    void load();
  }

  // Value-set picker options. Tier 2 prepends a Union sentinel (id=null)
  // so it can be the default; tier 3a omits it because the union there
  // arbitrarily picks one label per code (the original 3a footgun).
  // Sentinel typed as `ValueSetGroup | null` so `Picker` keeps its
  // generic identity helpers working uniformly.
  type VsOption = { kind: "union" } | { kind: "group"; group: ValueSetGroup };

  function vsOptions(data: ColumnValuesResponse): VsOption[] {
    const groups: VsOption[] = data.value_sets.map((g) => ({
      kind: "group" as const,
      group: g,
    }));
    if (data.tier === "2") {
      return [{ kind: "union" }, ...groups];
    }
    return groups;
  }

  function vsActive(data: ColumnValuesResponse): VsOption | null {
    if (data.picked_value_set === null) {
      return data.tier === "2" ? { kind: "union" } : null;
    }
    const hit = data.value_sets.find(
      (g) => g.value_set_id === data.picked_value_set,
    );
    return hit ? { kind: "group", group: hit } : null;
  }

  function vsKey(opt: VsOption): string {
    return opt.kind === "union" ? "union" : `vs:${opt.group.value_set_id}`;
  }

  function vsLabel(opt: VsOption): string {
    if (opt.kind === "union") return "Union";
    return yearRangeLabel(opt.group);
  }

  function yearRangeLabel(g: ValueSetGroup): string {
    if (g.year_min === null || g.year_max === null) return "no year info";
    if (g.year_min === g.year_max) return `${g.year_min}`;
    return `${g.year_min}–${g.year_max}`;
  }

  /** Cap on source names rendered in "applies to" before collapsing to
   * "+N more". Keeps the line on one or two visual rows for typical
   * project sizes; the user can drill into config to see the rest. */
  const APPLIES_TO_CAP = 6;

  /** "applies to" text for the currently selected option, scoped to the
   * project's sources rather than every cvid regmeta knows about. A
   * project source matches a value-set group iff its detected year
   * falls within [year_min, year_max]; yearless project sources are
   * mentioned separately because we can't place them on the timeline. */
  function vsActiveDescription(opt: VsOption | null): string | null {
    if (opt === null || opt.kind === "union") return null;
    const g = opt.group;
    const entries = Object.entries(sourceYears);
    if (entries.length === 0) return null;
    const inRange: string[] = [];
    let yearless = 0;
    for (const [name, year] of entries) {
      if (year === null) {
        yearless += 1;
        continue;
      }
      if (
        g.year_min !== null &&
        g.year_max !== null &&
        year >= g.year_min &&
        year <= g.year_max
      ) {
        inRange.push(`${name} (${year})`);
      }
    }
    if (inRange.length === 0) {
      // The group's year window doesn't intersect anything we loaded.
      // Surfacing this is informative: it lets the user know the picker
      // option is regmeta-only context, not actionable for their data.
      if (yearless > 0) {
        return `no project source falls in this window (${yearless} source${yearless === 1 ? "" : "s"} with unknown year)`;
      }
      return "no project source falls in this window";
    }
    inRange.sort();
    const head = inRange.slice(0, APPLIES_TO_CAP);
    const overflow = inRange.length - head.length;
    const overflowSuffix = overflow > 0 ? `, +${overflow} more` : "";
    const yearlessSuffix =
      yearless > 0
        ? ` (${yearless} other source${yearless === 1 ? "" : "s"} with unknown year)`
        : "";
    return `applies to ${head.join(", ")}${overflowSuffix}${yearlessSuffix}`;
  }

  function pickValueSet(opt: VsOption): void {
    const next = opt.kind === "union" ? null : opt.group.value_set_id;
    if (next === pickedValueSet) return;
    pickedValueSet = next;
    void load();
  }
</script>

<Modal headingId="value-codes-heading" {onClose}>
  <header>
    <div class="heading-stack">
      <span class="meta-line">
        value codes
        {#if register}· <span class="register-name">{register}</span>{/if}
      </span>
      <h3 id="value-codes-heading" class="mono">{column}</h3>
      {#if loadState.kind === "ok" && loadState.data.kind !== "none"}
        <span class="kind-tag kind-{loadState.data.kind}" title={loadState.data.kind}>
          {loadState.data.kind === "classification"
            ? `classification · ${loadState.data.title}`
            : `value codes · ${loadState.data.codes.length}`}
        </span>
      {/if}
    </div>
    <button type="button" class="close" aria-label="Close" onclick={onClose}>
      ×
    </button>
  </header>

  {#if loadState.kind === "loading"}
    <p class="status">Loading…</p>
  {:else if loadState.kind === "error"}
    <p class="status error">
      Could not load values: {loadState.message}
      <button type="button" class="retry" onclick={() => void load()}>
        Retry
      </button>
    </p>
  {:else if loadState.data.kind === "none"}
    <p class="status muted">
      regmeta has no value codes for <code>{column}</code>{register
        ? ` under ${register}`
        : ""}.
    </p>
  {:else}
    {#if loadState.data.note}
      <p class="variance-note variance-{loadState.data.tier ?? '1'}">
        {loadState.data.note}
      </p>
    {/if}
    {#if loadState.data.kind === "classification" && loadState.data.classifications.length > 1}
      {@const picked =
        loadState.data.picked_classification ?? loadState.data.classifications[0]}
      <Picker
        label="Classification:"
        options={loadState.data.classifications}
        value={picked}
        optionLabel={(sn) => sn}
        eqKey={(sn) => sn}
        onPick={pickClassification}
      />
    {/if}
    {#if loadState.data.value_sets.length > 1}
      {@const opts = vsOptions(loadState.data)}
      {@const active = vsActive(loadState.data)}
      <Picker
        label="Value-set:"
        options={opts}
        value={active}
        optionLabel={vsLabel}
        eqKey={vsKey}
        activeDescription={vsActiveDescription(active)}
        onPick={pickValueSet}
      />
    {/if}
    {#if loadState.data.description}
      <p class="description">{loadState.data.description}</p>
    {/if}
    <div class="codes-wrap">
      <table class="codes">
        <colgroup>
          <col class="col-code" />
          <col class="col-label" />
        </colgroup>
        <thead>
          <tr>
            <th>Code</th>
            <th>Label</th>
          </tr>
        </thead>
        <tbody>
          {#each loadState.data.codes as c (c.code)}
            <tr>
              <td class="mono">{c.code}</td>
              <td>{c.label ?? ""}</td>
            </tr>
          {/each}
        </tbody>
      </table>
    </div>
  {/if}

  <footer>
    <button type="button" onclick={onClose}>Close</button>
  </footer>
</Modal>

<style>
  header {
    display: flex;
    justify-content: space-between;
    align-items: flex-start;
    gap: 1rem;
  }
  .heading-stack {
    display: flex;
    flex-direction: column;
    gap: 0.2rem;
    min-width: 0;
    flex: 1 1 auto;
  }
  .meta-line {
    color: #777;
    font-size: 0.82rem;
  }
  .register-name {
    color: #1a3b80;
    font-weight: 500;
  }
  h3 {
    margin: 0;
    font-size: 1.05rem;
    word-break: break-word;
  }
  .mono {
    font-family: ui-monospace, monospace;
  }
  .kind-tag {
    display: inline-block;
    align-self: flex-start;
    padding: 0.05rem 0.4rem;
    border-radius: 3px;
    font-size: 0.78rem;
    background: #f0e8fa;
    color: #5d2b8c;
  }
  .close {
    background: transparent;
    border: 0;
    font-size: 1.4rem;
    cursor: pointer;
    color: #666;
    flex: 0 0 auto;
    padding: 0;
    line-height: 1;
  }
  .status {
    margin: 0;
    color: #555;
  }
  .status.muted {
    color: #888;
  }
  .status.error {
    background: #fde8e8;
    border: 1px solid #e0a0a0;
    color: #882020;
    padding: 0.5rem 0.75rem;
    border-radius: 4px;
  }
  .retry {
    margin-left: 0.5rem;
    border: 1px solid currentColor;
    background: transparent;
    color: inherit;
    padding: 0.15rem 0.5rem;
    border-radius: 3px;
    cursor: pointer;
    font: inherit;
  }
  /* Variance notes (issue #64). Tier 2 = code set differs across years
     but labels are stable. Tier 3a = same code, different labels (the
     dangerous one). Tier 3b = different classifications (paired with
     the picker). Color escalates from amber to red so the eye lands on
     3a / 3b without reading the text first. */
  .variance-note {
    margin: 0 0 0.4rem;
    padding: 0.35rem 0.6rem;
    border-radius: 4px;
    font-size: 0.85rem;
    line-height: 1.35;
    border: 1px solid #e8c184;
    background: #fff7e6;
    color: #7a4a00;
  }
  .variance-3a {
    border-color: #e0a0a0;
    background: #fde8e8;
    color: #882020;
  }
  .variance-3b {
    border-color: #e8c184;
    background: #fff7e6;
    color: #7a4a00;
  }
  .description {
    margin: 0;
    color: #555;
    font-size: 0.9rem;
  }
  /* Code list often runs to hundreds of rows for big classifications
     (SUN, SSYK, …). Cap height + scroll inside so the modal stays at a
     usable size and the Close button remains reachable. */
  .codes-wrap {
    max-height: 22rem;
    overflow: auto;
    border: 1px solid #e1e1e1;
    border-radius: 4px;
  }
  .codes {
    width: 100%;
    border-collapse: collapse;
    font-size: 0.88rem;
    table-layout: fixed;
  }
  .codes th,
  .codes td {
    text-align: left;
    padding: 0.25rem 0.5rem;
    border-bottom: 1px solid #f0f0f0;
    vertical-align: top;
    overflow-wrap: anywhere;
  }
  .codes th {
    position: sticky;
    top: 0;
    background: #f7f7f9;
    color: #555;
    font-weight: 500;
    font-size: 0.78rem;
    text-transform: uppercase;
    letter-spacing: 0.04em;
  }
  .col-code {
    width: 8rem;
  }
  footer {
    display: flex;
    justify-content: flex-end;
  }
  button {
    padding: 0.4rem 0.9rem;
    border-radius: 4px;
    border: 1px solid #ccc;
    background: #fff;
    cursor: pointer;
    font: inherit;
  }
</style>
