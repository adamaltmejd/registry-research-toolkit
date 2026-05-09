<script lang="ts">
  import { untrack } from "svelte";

  import { store } from "../store.svelte";
  import type { ColumnInfo, ColumnType } from "../types";
  import Modal from "./Modal.svelte";

  interface Props {
    /** Sources making up the partition the user clicked. The scope
     *  picker can narrow this to one or widen it to the whole register
     *  (when registerSources is a strict superset). */
    sources: string[];
    /** All sources in the surrounding register. When equal in length to
     *  `sources`, the partition spans the whole register (no
     *  reconcile-all option needed). */
    registerSources: string[];
    /** Cosmetic — used in modal copy. */
    registerName: string | null;
    column: ColumnInfo;
    onClose: () => void;
  }

  let { sources, registerSources, registerName, column, onClose }: Props =
    $props();

  const TYPES: ColumnType[] = ["id", "categorical", "numeric", "opaque", "date"];

  // Three scope choices map to three explicit user intents:
  //   - "partition": apply to all sources in this row's variant. Default
  //     for both fully-agreeing rows (= the whole register) and
  //     disagreement rows (= just this variant's sources).
  //   - "single": apply to one source (picked from the partition).
  //   - "register": reconcile every source in the register to this type.
  //     Only offered when the partition is a strict subset of the
  //     register (i.e. sibling variants exist).
  type Scope = "partition" | "single" | "register";

  // Scope picker is meaningful only when the register has more than one
  // source. With one source the only valid target is that source.
  let showScopePicker = $derived(registerSources.length > 1);
  // "Reconcile across the whole register" is only useful when the
  // partition is a strict subset (sibling variants exist).
  let canReconcileAll = $derived(sources.length < registerSources.length);
  // Default scope: when the partition has multiple sources, the user
  // clicked the row to edit them together — keep that intent. When the
  // partition is one source, there's nothing else to bulk-edit, so
  // "single" is the natural default.
  let scope: Scope = $state(
    untrack(() => (sources.length > 1 ? "partition" : "single")),
  );
  let singleSource: string = $state(
    untrack(() => (sources.length > 0 ? sources[0] : "")),
  );

  let effectiveSources = $derived.by(() => {
    if (scope === "single") return singleSource ? [singleSource] : [];
    if (scope === "register") return [...registerSources];
    return [...sources];
  });

  // SCB register names are usually "Long descriptive name (ACRONYM)".
  // The full name is fine in the modal subline (which can wrap onto
  // multiple lines), but inside a radio label it stretches the form
  // unreadably wide. Pull out the trailing ALL-CAPS acronym when
  // present; tooltip carries the full name. Falls back to the full
  // name (truncated by CSS) when there's no parenthetical to extract.
  function shortRegisterName(full: string | null): string | null {
    if (!full) return null;
    const m = full.match(/\(([A-ZÅÄÖ0-9][A-ZÅÄÖ0-9 -]{1,15})\)\s*$/);
    return m ? m[1] : full;
  }
  let registerShort = $derived(shortRegisterName(registerName));

  // Modal editor: snapshot the prop once on mount and let local edit
  // state diverge. `untrack` signals to the compiler that not reacting
  // to upstream changes is intentional.
  let selectedType: ColumnType = $state(untrack(() => column.current_type));
  let idSubtype: string = $state(
    untrack(() => (column.hint?.id_subtype as string | undefined) ?? ""),
  );
  let numericSubtype: string = $state(
    untrack(() => (column.hint?.numeric_subtype as string | undefined) ?? ""),
  );
  let dateFormat: string = $state(
    untrack(() => (column.hint?.date_format as string | undefined) ?? ""),
  );
  let submitting = $state(false);

  function buildHint(): Record<string, unknown> | null {
    // Always send an explicit hint based on form state. Earlier we
    // returned `undefined` to preserve the server's existing value
    // when type was unchanged, but that made the "unset" option in
    // the dropdown a no-op — users couldn't clear an existing hint
    // without first changing the type. Explicit-always trades the
    // UNCHANGED micro-optimization for a form that actually does
    // what it says.
    if (selectedType === "id" && idSubtype) return { id_subtype: idSubtype };
    if (selectedType === "numeric" && numericSubtype)
      return { numeric_subtype: numericSubtype };
    if (selectedType === "date" && dateFormat)
      return { date_format: dateFormat };
    return null;
  }

  async function submit(event: SubmitEvent): Promise<void> {
    event.preventDefault();
    if (submitting) return;
    const version = store.snapshot?.snapshot_version;
    if (!version) return;
    submitting = true;
    const targets = effectiveSources;
    const ok = await store.setColumnType({
      sources: targets,
      column: column.name,
      type: selectedType,
      expected_version: version,
      hint: buildHint(),
    });
    submitting = false;
    if (ok) {
      const n = targets.length;
      store.pushToast(
        "info",
        n === 1
          ? `Saved ${column.name} → ${selectedType}`
          : `Saved ${column.name} → ${selectedType} (${n} sources)`,
      );
      onClose();
    }
  }
</script>

<Modal headingId="column-type-editor-heading" {onClose}>
  <form onsubmit={submit}>
    <header>
      <div class="heading-stack">
        {#if sources.length === 1}
          <span class="source-line" title={sources[0]}>{sources[0]}</span>
        {:else}
          <span class="source-line bulk">
            applying to {sources.length} sources{registerShort
              ? " in "
              : ""}{#if registerShort}<span
                class="register-name"
                title={registerName ?? undefined}>{registerShort}</span
              >{/if}
          </span>
        {/if}
        <h3 id="column-type-editor-heading">{column.name}</h3>
        {#if sources.length > 1}
          <details class="source-list-detail">
            <summary>show source names</summary>
            <ul>
              {#each sources as sn (sn)}
                <li class="mono">{sn}</li>
              {/each}
            </ul>
          </details>
        {/if}
      </div>
      <button type="button" class="close" aria-label="Close" onclick={onClose}>
        ×
      </button>
    </header>

    {#if column.regmeta_signal}
      <p class="regmeta-context" aria-label="regmeta context">
        regmeta:
        {#if column.regmeta_signal.classification_short_name}
          <code>{column.regmeta_signal.classification_short_name}</code>
        {/if}
        {#if column.regmeta_signal.has_value_codes}
          {#if column.regmeta_signal.classification_short_name}·{/if}
          value codes available
        {/if}
        {#if column.regmeta_signal.datatyp_kind}
          · datatype <code>{column.regmeta_signal.datatyp_kind}</code>
        {/if}
        {#if !column.regmeta_signal.classification_short_name && !column.regmeta_signal.has_value_codes && !column.regmeta_signal.datatyp_kind}
          column known to regmeta but with no classification, codes, or
          datatype hint.
        {/if}
      </p>
    {:else}
      <p class="regmeta-context regmeta-missing">
        regmeta: no record for this column name.
      </p>
    {/if}

    {#if showScopePicker}
      <fieldset class="scope">
        <legend>Apply to</legend>
        {#if sources.length > 1}
          <label class="radio">
            <input
              type="radio"
              name="scope"
              value="partition"
              bind:group={scope}
            />
            All {sources.length} sources in this variant
          </label>
        {/if}
        <label class="radio">
          <input type="radio" name="scope" value="single" bind:group={scope} />
          Only
          <select bind:value={singleSource} aria-label="single source">
            {#each sources as sn (sn)}
              <option value={sn}>{sn}</option>
            {/each}
          </select>
        </label>
        {#if canReconcileAll}
          <label class="radio">
            <input
              type="radio"
              name="scope"
              value="register"
              bind:group={scope}
            />
            All {registerSources.length} sources in
            <span class="register-name" title={registerName ?? undefined}
              >{registerShort ?? "this register"}</span
            >
            <span class="hint">· reconcile</span>
          </label>
        {/if}
      </fieldset>
    {/if}

    <fieldset>
      <legend>Type</legend>
      {#each TYPES as t (t)}
        <label class="radio">
          <input type="radio" name="type" value={t} bind:group={selectedType} />
          {t}
          {#if t === column.regmeta_implied_type}
            <span class="hint" title="regmeta-implied type for this column"
              >· regmeta</span
            >
          {/if}
        </label>
      {/each}
    </fieldset>

    {#if selectedType === "id"}
      <label class="row">
        <span>id_subtype</span>
        <select bind:value={idSubtype}>
          <option value="">(unset — sample at extract)</option>
          <option value="integer">integer</option>
          <option value="string">string</option>
        </select>
      </label>
    {:else if selectedType === "numeric"}
      <label class="row">
        <span>numeric_subtype</span>
        <select bind:value={numericSubtype}>
          <option value="">(unset — sample at extract)</option>
          <option value="integer">integer</option>
          <option value="double">double</option>
        </select>
      </label>
    {:else if selectedType === "date"}
      <label class="row">
        <span>date_format</span>
        <input
          type="text"
          placeholder="e.g. %Y%m%d"
          bind:value={dateFormat}
          spellcheck="false"
        />
      </label>
    {/if}

    <footer>
      <button type="button" onclick={onClose} disabled={submitting}
        >Cancel</button
      >
      <button
        type="submit"
        class="primary"
        disabled={submitting || effectiveSources.length === 0}
      >
        {#if submitting}
          Saving…
        {:else if effectiveSources.length > 1}
          Save · {effectiveSources.length} sources
        {:else}
          Save
        {/if}
      </button>
    </footer>
  </form>
</Modal>

<style>
  form {
    display: flex;
    flex-direction: column;
    gap: 0.75rem;
  }
  header {
    display: flex;
    justify-content: space-between;
    align-items: flex-start;
    gap: 1rem;
  }
  .heading-stack {
    display: flex;
    flex-direction: column;
    gap: 0.15rem;
    min-width: 0;
    flex: 1 1 auto;
  }
  .source-line {
    color: #777;
    font-size: 0.85rem;
    font-family: ui-monospace, monospace;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
  }
  .source-line.bulk {
    font-family: system-ui, sans-serif;
    color: #555;
    font-weight: 500;
    white-space: normal;
    overflow: visible;
    text-overflow: unset;
  }
  .source-list-detail {
    margin-top: 0.15rem;
    font-size: 0.82rem;
    color: #666;
  }
  .source-list-detail > summary {
    cursor: pointer;
    list-style: revert;
    user-select: none;
  }
  .source-list-detail ul {
    max-height: 8rem;
    overflow: auto;
    margin: 0.3rem 0 0;
    padding: 0 0 0 1.2rem;
  }
  .source-list-detail .mono {
    font-family: ui-monospace, monospace;
    font-size: 0.85rem;
  }
  h3 {
    margin: 0;
    font-size: 1.05rem;
    font-family: ui-monospace, monospace;
    word-break: break-word;
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
  .regmeta-context {
    margin: 0;
    padding: 0.4rem 0.6rem;
    background: #f4f6fb;
    border-left: 3px solid #c8d3ec;
    font-size: 0.85rem;
    color: #444;
    border-radius: 3px;
  }
  .regmeta-context code {
    background: #fff;
    padding: 0.05rem 0.3rem;
    border-radius: 3px;
    font-size: 0.95em;
  }
  .regmeta-missing {
    background: #fff8e9;
    border-left-color: #f0c14b;
    color: #5b4a14;
  }
  fieldset {
    border: 1px solid #ddd;
    border-radius: 4px;
    padding: 0.5rem 0.75rem;
    margin: 0;
  }
  .scope {
    background: #fafbff;
  }
  .scope .radio select {
    margin-left: 0.3rem;
  }
  /* Acronym-with-tooltip pattern: extracted "(LISA)" form fits on the
     line; long unabbreviated names stay intact but get cut off with
     ellipsis, keeping the radio label single-line. The full register
     name is always available via the title tooltip. */
  .register-name {
    display: inline-block;
    max-width: 22rem;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    vertical-align: bottom;
    border-bottom: 1px dotted rgba(0, 0, 0, 0.25);
    cursor: help;
  }
  legend {
    padding: 0 0.25rem;
    font-size: 0.85rem;
    color: #666;
  }
  .radio {
    display: block;
    padding: 0.15rem 0;
    cursor: pointer;
  }
  .hint {
    color: #888;
    font-size: 0.85rem;
  }
  .row {
    display: grid;
    grid-template-columns: 9rem 1fr;
    align-items: center;
    gap: 0.5rem;
  }
  .row span {
    color: #666;
    font-size: 0.9rem;
  }
  input[type="text"],
  select {
    padding: 0.3rem 0.5rem;
    border: 1px solid #ccc;
    border-radius: 4px;
    font: inherit;
  }
  footer {
    display: flex;
    justify-content: flex-end;
    gap: 0.5rem;
  }
  button {
    padding: 0.4rem 0.9rem;
    border-radius: 4px;
    border: 1px solid #ccc;
    background: #fff;
    cursor: pointer;
    font: inherit;
  }
  button.primary {
    background: #1656c0;
    color: #fff;
    border-color: #1656c0;
  }
  button:disabled {
    opacity: 0.6;
    cursor: not-allowed;
  }
</style>
