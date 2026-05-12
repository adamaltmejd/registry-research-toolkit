<script lang="ts">
  import { untrack } from "svelte";

  import type { ColumnVarinfoResponse, VarinfoDescription } from "../api";
  import {
    columnIsManual,
    hasRegmetaValueDisplay,
    relevantYearsFromMap,
    sourceYearInfoFor,
    store,
  } from "../store.svelte";
  import type { ColumnInfo, ColumnType } from "../types";
  import Modal from "./Modal.svelte";
  import ValueCodesPanel from "./ValueCodesPanel.svelte";

  interface Props {
    /** Sources making up the partition the user clicked. The scope
     *  picker can narrow this to one or widen it to every carrier in
     *  `cellBySource` (when that's a strict superset). */
    sources: string[];
    /** Carrier source → ColumnInfo for the edited column. Keys are
     *  every source in the surrounding register that actually carries
     *  the column — the server validates every (source, column) pair
     *  and would reject a call that includes a source missing it. */
    cellBySource: Record<string, ColumnInfo>;
    /** Cosmetic — used in modal copy. */
    registerName: string | null;
    column: ColumnInfo;
    onClose: () => void;
  }

  let { sources, cellBySource, registerName, column, onClose }: Props = $props();

  let registerSourcesWithColumn = $derived(Object.keys(cellBySource));

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

  // Scope picker is meaningful only when more than one carrier source
  // exists. With one source the only valid target is that source.
  let showScopePicker = $derived(registerSourcesWithColumn.length > 1);
  // "Reconcile across the whole register" is only useful when the
  // partition is a strict subset of the carrier set (sibling variants
  // exist for the same column).
  let canReconcileAll = $derived(
    sources.length < registerSourcesWithColumn.length,
  );
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
    if (scope === "register") return [...registerSourcesWithColumn];
    return [...sources];
  });

  // Manual-override count in current scope; drives the Unset button.
  let manualInScopeCount = $derived(
    effectiveSources
      .map((sn) => cellBySource[sn])
      .filter((c): c is ColumnInfo => c !== undefined)
      .filter(columnIsManual).length,
  );

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

  type VarinfoState =
    | { kind: "loading" }
    | { kind: "loaded"; data: ColumnVarinfoResponse }
    | { kind: "error"; message: string };
  let varinfoState: VarinfoState = $state({ kind: "loading" });
  let showAlternatives = $state(false);
  // When the user clicks an alternative in the varinfo popup we swap
  // the value-codes panel to that variable's scope without re-running
  // the primary picker. ``null`` means "use whatever the server's
  // primary is" — the panel falls back to the response's primary
  // ``var_id`` then.
  let userPickedVarId: number | null = $state(null);

  // Scoped to `sources` (the partition the user clicked), not to every
  // carrier in the register. SCB reuses column-name slots across eras —
  // pulling sibling-era years into `relevantYears` would defeat the
  // year-aware ranking and surface the wrong-era variable as primary.
  // Both varinfo lookup and value-codes filtering read this map.
  let sourceYearInfo = $derived(sourceYearInfoFor(sources));
  let sourceYears = $derived(
    Object.fromEntries(
      sources.map((sn) => [sn, sourceYearInfo[sn].year]),
    ),
  );
  let relevantYears = $derived(relevantYearsFromMap(sourceYears));
  // True when *every* source in the partition is in the "missing" state
  // — i.e. auto-detection failed and the user hasn't intervened. Drives
  // the "set a year" warning that replaces the description / value-code
  // body when regmeta would otherwise mix wrong-era variables.
  let allYearsUnknown = $derived(
    sources.every((sn) => sourceYearInfo[sn].provenance === "missing"),
  );

  async function loadVarinfo(): Promise<void> {
    try {
      const data = await store.getColumnVarinfo(
        registerName,
        column.name,
        relevantYears,
      );
      varinfoState = { kind: "loaded", data };
    } catch (exc) {
      const message = exc instanceof Error ? exc.message : String(exc);
      varinfoState = { kind: "error", message };
    }
  }

  // Depend on the join'd key so $effect re-fires only when the year set
  // changes value (not on every Array reassignment).
  let varinfoYearKey = $derived(relevantYears.join(","));
  $effect(() => {
    varinfoYearKey;
    // Clear any explicit alternative pick when the year-driven primary
    // re-ranks — otherwise the previous-year pick would override the
    // new year-aware primary.
    userPickedVarId = null;
    void loadVarinfo();
  });

  function varSourceLine(d: VarinfoDescription): string {
    return d.register_name
      ? `var_id ${d.var_id} in ${d.register_name}`
      : `var_id ${d.var_id}`;
  }

  // SCB's ``VariabelRegister_Källa`` is meant to name an upstream register
  // (LISA-style "RTB", "RAMS"); when it's just an echo of the column or
  // variable name it carries no info and clutters the panel.
  function meaningfulRegisterSource(
    raw: string | null,
    columnName: string,
    variabelnamn: string | null,
  ): string | null {
    if (!raw) return null;
    const trimmed = raw.trim();
    if (!trimmed) return null;
    const norm = trimmed.toLowerCase();
    if (norm === columnName.toLowerCase()) return null;
    if (variabelnamn && norm === variabelnamn.toLowerCase()) return null;
    return trimmed;
  }

  // SCB often fills ``Variabeldefinition`` with the same text as
  // ``Variabelnamn`` (sometimes with a trailing period). Surfacing both
  // reads as a typo. Normalise trailing punctuation + whitespace before
  // comparing so the definition only appears when it adds info.
  function meaningfulDefinition(
    definition: string | null,
    variabelnamn: string | null,
  ): string | null {
    if (!definition) return null;
    const trimmed = definition.trim();
    if (!trimmed) return null;
    if (!variabelnamn) return trimmed;
    const norm = (s: string): string =>
      s.trim().replace(/[.。!?]+$/u, "").toLowerCase();
    if (norm(trimmed) === norm(variabelnamn)) return null;
    return trimmed;
  }

  // Active variable: explicit user pick wins, otherwise the server's
  // primary. Null in non-loaded states.
  let activeDescription = $derived.by((): VarinfoDescription | null => {
    if (varinfoState.kind !== "loaded") return null;
    const data = varinfoState.data;
    if (data.kind === "none") return null;
    if (userPickedVarId === null) return data.primary;
    if (data.kind === "single") return data.primary;
    const alt = data.alternatives.find(
      (a) => a.description.var_id === userPickedVarId,
    );
    return alt ? alt.description : data.primary;
  });
  let activeVarId = $derived(activeDescription?.var_id ?? null);
  // Cvid count for the active variable. The banner reads "(N of M cvids)"
  // and must track user picks, not just the server's primary.
  let activeInstances = $derived.by((): number | null => {
    if (varinfoState.kind !== "loaded") return null;
    const data = varinfoState.data;
    if (data.kind === "none") return null;
    if (activeVarId === null) return null;
    if (data.primary.var_id === activeVarId) return data.primary_share.instances;
    if (data.kind !== "divergent") return null;
    const alt = data.alternatives.find((a) => a.description.var_id === activeVarId);
    return alt ? alt.instances : null;
  });

  let canShowValueCodes = $derived(hasRegmetaValueDisplay(column.regmeta_signal));
  // Mount ValueCodesPanel on first expand and keep it mounted; <details>
  // hides it via CSS when closed. Re-mounting would re-fire the
  // /api/column-values request on every toggle.
  let valueCodesEverExpanded = $state(store.valueCodesExpandedInEditor);

  // When the year is unset AND the variable / value-code surface is
  // divergent (multiple var_ids OR multiple value-set windows under one
  // register), refuse to display description and codes — mixing
  // wrong-era variants would mislead the user. The Edit register popup
  // is where the year gets fixed; this gate just routes the user there.
  let varinfoDivergent = $derived.by(() => {
    if (varinfoState.kind !== "loaded") return false;
    return varinfoState.data.kind === "divergent";
  });
  let valueCodesAcrossYears = $derived(
    (column.regmeta_signal?.n_value_sets ?? 0) > 1 ||
      (column.regmeta_signal?.n_classifications ?? 0) > 1,
  );
  let yearConflictGated = $derived(
    allYearsUnknown && (varinfoDivergent || valueCodesAcrossYears),
  );

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

  async function unsetManual(): Promise<void> {
    if (submitting) return;
    const version = store.snapshot?.snapshot_version;
    if (!version) return;
    // Server silently skips non-manual pairs, so we send the whole scope.
    // Snapshot the count before await — the snapshot may advance under
    // us and re-derive `manualInScopeCount` against the cleared state.
    const n = manualInScopeCount;
    submitting = true;
    const ok = await store.unsetColumnManual({
      sources: effectiveSources,
      column: column.name,
      expected_version: version,
    });
    submitting = false;
    if (ok) {
      store.pushToast(
        "info",
        n === 1
          ? `Cleared manual override on ${column.name}`
          : `Cleared ${n} manual overrides on ${column.name}`,
      );
      onClose();
    }
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

    <div class="modal-body">
      {#if yearConflictGated}
        <section class="year-conflict" aria-label="year required">
          <p class="year-conflict-msg">
            <strong>⚠ Year not set</strong> — under
            {registerShort ?? registerName ?? "this register"} this column
            aliases to
            {#if varinfoDivergent && varinfoState.kind === "loaded" && varinfoState.data.kind === "divergent"}
              {varinfoState.data.alternatives.length + 1} variables
            {/if}
            {#if varinfoDivergent && valueCodesAcrossYears} and {/if}
            {#if valueCodesAcrossYears}
              value codes that shifted across years
            {/if}.
            Without a year we can't tell which variant applies, so
            descriptions and value codes are hidden.
          </p>
          <p class="year-conflict-action">
            Set the year in <strong>Edit register</strong> on
            {registerShort ?? registerName ?? "this group"}.
          </p>
        </section>
      {/if}
      <section
        class="varinfo"
        class:varinfo-gated={yearConflictGated}
        aria-label="regmeta variable description"
        aria-hidden={yearConflictGated}
        hidden={yearConflictGated}
      >
        {#if varinfoState.kind === "loading"}
          <p class="varinfo-status">Loading variable info…</p>
        {:else if varinfoState.kind === "error"}
          <p class="varinfo-status varinfo-error">
            Could not load variable info: {varinfoState.message}
            <button
              type="button"
              class="retry"
              onclick={() => {
                varinfoState = { kind: "loading" };
                void loadVarinfo();
              }}>Retry</button
            >
          </p>
        {:else if varinfoState.data.kind === "none"}
          <p class="varinfo-status varinfo-none">
            {#if varinfoState.data.reason === "no_register"}
              No register pinned — assign one to see variable info
            {:else if varinfoState.data.reason === "unavailable"}
              Variable info unavailable (regmeta not installed)
            {:else}
              Variable: not described in regmeta
            {/if}
          </p>
        {:else}
          {@const data = varinfoState.data}
          {@const desc = activeDescription ?? data.primary}
          {@const isUserPick =
            data.kind === "divergent" &&
            userPickedVarId !== null &&
            userPickedVarId !== data.primary.var_id}
          {@const registerSource = meaningfulRegisterSource(
            desc.variabelregister_kalla,
            column.name,
            desc.variabelnamn,
          )}
          {@const definitionText = meaningfulDefinition(
            desc.variabeldefinition,
            desc.variabelnamn,
          )}
          <div class="varinfo-body">
            <p class="varinfo-name">
              <strong>{desc.variabelnamn ?? column.name}</strong>
              {#if isUserPick}
                <span
                  class="varinfo-userpick"
                  title="you picked this alternative from the list below"
                >
                  · your pick
                </span>
              {/if}
              {#if data.kind === "divergent"}
                <span
                  class="varinfo-warn"
                  title="this column aliases to more than one variable under {registerName ??
                    'this register'}"
                >
                  ⚠ also aliases to {data.alternatives.length} other variable{data
                    .alternatives.length === 1
                    ? ""
                    : "s"}
                </span>
              {/if}
            </p>
            {#if definitionText}
              <p class="varinfo-definition">
                {definitionText}
                {#if data.kind === "divergent" && activeInstances !== null}
                  <span class="varinfo-share">
                    ({activeInstances} of {data.primary_share.total}
                    cvids)
                  </span>
                {/if}
              </p>
            {:else if data.kind === "divergent" && activeInstances !== null}
              <p class="varinfo-share">
                ({activeInstances} of {data.primary_share.total} cvids)
              </p>
            {/if}
            <dl class="varinfo-fields">
              {#if desc.variabelbeskrivning}
                <dt>Description</dt>
                <dd>{desc.variabelbeskrivning}</dd>
              {/if}
              {#if desc.variabeloperationell_definition}
                <dt>Definition</dt>
                <dd>{desc.variabeloperationell_definition}</dd>
              {/if}
              {#if desc.mattenhet}
                <dt>Unit</dt>
                <dd>{desc.mattenhet}</dd>
              {/if}
              {#if desc.variabelreferenstid}
                <dt>Reference time</dt>
                <dd>{desc.variabelreferenstid}</dd>
              {/if}
              {#if desc.variabelhamtadfran}
                <dt>Sourced from</dt>
                <dd>{desc.variabelhamtadfran}</dd>
              {/if}
              {#if registerSource}
                <dt>Register source</dt>
                <dd>{registerSource}</dd>
              {/if}
              <dt>Source</dt>
              <dd>{varSourceLine(desc)}</dd>
            </dl>
            {#if data.kind === "divergent"}
              {@const primaryVarId = data.primary.var_id}
              <details
                class="varinfo-more"
                bind:open={showAlternatives}
              >
                <summary>
                  Pick a different variable
                  ({data.alternatives.length + 1} options)
                </summary>
                <p class="varinfo-pick-help">
                  Click a variable below to scope the value codes to its
                  instances. SCB reuses column slots across decades, so the
                  picker's automatic choice can land on the wrong era.
                </p>
                <ul class="varinfo-alternatives">
                  <li>
                    <button
                      type="button"
                      class="varinfo-pick"
                      class:varinfo-pick-active={userPickedVarId === null ||
                        userPickedVarId === primaryVarId}
                      onclick={() => (userPickedVarId = null)}
                    >
                      <p class="varinfo-name">
                        <strong>{data.primary.variabelnamn ?? "(unnamed)"}</strong>
                        <span class="varinfo-share">
                          · primary · {data.primary_share.instances} cvid{data
                            .primary_share.instances === 1
                            ? ""
                            : "s"}
                          · {varSourceLine(data.primary)}
                        </span>
                      </p>
                      {#if data.primary.variabeldefinition}
                        <p class="varinfo-definition">
                          {data.primary.variabeldefinition}
                        </p>
                      {/if}
                    </button>
                  </li>
                  {#each data.alternatives as alt (alt.description.var_id)}
                    <li>
                      <button
                        type="button"
                        class="varinfo-pick"
                        class:varinfo-pick-active={userPickedVarId ===
                          alt.description.var_id}
                        onclick={() =>
                          (userPickedVarId = alt.description.var_id)}
                      >
                        <p class="varinfo-name">
                          <strong>{alt.description.variabelnamn ?? "(unnamed)"}</strong>
                          <span class="varinfo-share">
                            · {alt.instances} cvid{alt.instances === 1 ? "" : "s"}
                            · {varSourceLine(alt.description)}
                          </span>
                        </p>
                        {#if alt.description.variabeldefinition}
                          <p class="varinfo-definition">
                            {alt.description.variabeldefinition}
                          </p>
                        {/if}
                      </button>
                    </li>
                  {/each}
                </ul>
              </details>
            {/if}
          </div>
        {/if}
      </section>

      {#if !yearConflictGated && column.regmeta_signal}
        {@const sig = column.regmeta_signal}
        <!-- Surface datatype only when it's the active type signal.
             ``regmeta_implied_type`` gives value-codes / classification
             precedence: when either is present, the column is
             categorical and the underlying ``datatyp`` is a storage
             detail (e.g. Kommun: numeric-coded categorical → datatyp
             "numeric"). Showing "datatype numeric" on a categorical
             column reads as a contradiction. -->
        {@const showDatatype =
          !!sig.datatyp_kind &&
          !sig.classification_short_name &&
          !sig.has_value_codes}
        {@const showEmpty =
          !sig.classification_short_name &&
          !sig.has_value_codes &&
          !sig.datatyp_kind}
        {#if showDatatype || showEmpty}
          <p class="regmeta-context" aria-label="regmeta context">
            regmeta:
            {#if showDatatype}
              datatype <code>{sig.datatyp_kind}</code>
            {:else if showEmpty}
              column known to regmeta but with no classification, value codes,
              or datatype hint.
            {/if}
          </p>
        {/if}
      {/if}

      {#if !yearConflictGated && canShowValueCodes}
        <details
          class="value-codes-inline"
          open={store.valueCodesExpandedInEditor}
          ontoggle={(e) => {
            const open = (e.currentTarget as HTMLDetailsElement).open;
            store.setValueCodesExpandedInEditor(open);
            if (open) valueCodesEverExpanded = true;
          }}
        >
          <summary>
            {store.valueCodesExpandedInEditor
              ? "Hide value codes"
              : "Show value codes"}
          </summary>
          {#if valueCodesEverExpanded}
            <ValueCodesPanel
              register={registerName}
              column={column.name}
              {sourceYears}
              pickedVarId={activeVarId}
            />
          {/if}
        </details>
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
              All {registerSourcesWithColumn.length} sources in
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
    </div>

    <footer>
      {#if manualInScopeCount > 0}
        <button
          type="button"
          class="unset"
          onclick={unsetManual}
          disabled={submitting}
          title={`Drop the manual marker and re-run auto classification on ${manualInScopeCount} cell${manualInScopeCount === 1 ? "" : "s"} in scope`}
        >
          {#if manualInScopeCount === 1}
            Unset manual override
          {:else}
            Unset · {manualInScopeCount} manual
          {/if}
        </button>
      {/if}
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
  /* form + .modal-body flex/scroll layout is defined in Modal.svelte. */
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
  .varinfo {
    padding: 0.5rem 0.7rem;
    background: #fbfaf6;
    border-left: 3px solid #d8cfa8;
    border-radius: 3px;
    font-size: 0.9rem;
    color: #3a3528;
  }
  .varinfo-status {
    margin: 0;
    color: #666;
    font-style: italic;
  }
  .varinfo-status.varinfo-none {
    color: #888;
  }
  .varinfo-status.varinfo-error {
    background: #fde8e8;
    border: 1px solid #e0a0a0;
    color: #882020;
    padding: 0.35rem 0.55rem;
    border-radius: 3px;
    font-style: normal;
  }
  .varinfo-body {
    display: flex;
    flex-direction: column;
    gap: 0.45rem;
  }
  .varinfo-name {
    margin: 0;
    font-size: 1rem;
  }
  .varinfo-warn {
    margin-left: 0.4rem;
    color: #7a4a00;
    font-size: 0.85em;
  }
  .varinfo-userpick {
    margin-left: 0.3rem;
    color: #5d2b8c;
    font-size: 0.85em;
    font-weight: 500;
  }
  .varinfo-definition {
    margin: 0;
    line-height: 1.4;
  }
  .varinfo-share {
    color: #888;
    font-size: 0.85em;
  }
  /* Two-column label/value grid. ``dt`` is the muted label, ``dd`` is
     the content. Long values wrap inside their cell; the label column
     stays narrow and aligned. */
  .varinfo-fields {
    display: grid;
    grid-template-columns: max-content 1fr;
    column-gap: 0.75rem;
    row-gap: 0.3rem;
    margin: 0.15rem 0 0;
    font-size: 0.88rem;
    line-height: 1.4;
  }
  .varinfo-fields dt {
    color: #7a7158;
    font-size: 0.82rem;
    font-weight: 500;
    padding-top: 0.05rem;
    white-space: nowrap;
  }
  .varinfo-fields dd {
    margin: 0;
    color: #3a3528;
  }
  .varinfo-more {
    margin-top: 0.2rem;
    font-size: 0.85rem;
    color: #555;
  }
  .varinfo-more > summary {
    cursor: pointer;
    user-select: none;
    color: #555;
  }
  .varinfo-alternatives {
    margin: 0.4rem 0 0;
    padding-left: 0;
    list-style: none;
    display: flex;
    flex-direction: column;
    gap: 0.4rem;
  }
  .varinfo-pick-help {
    margin: 0.4rem 0 0;
    color: #6a614a;
    font-size: 0.82rem;
    line-height: 1.4;
  }
  /* Clickable variable cards. Resetting button defaults (background,
     padding, etc.) lets us reuse the same layout as the static list
     items while exposing native button affordances (focus ring,
     keyboard activation). */
  .varinfo-pick {
    display: block;
    width: 100%;
    text-align: left;
    background: #fff;
    border: 1px solid #e1d8b8;
    border-radius: 4px;
    padding: 0.35rem 0.55rem;
    cursor: pointer;
    font: inherit;
    color: inherit;
  }
  .varinfo-pick:hover:not(.varinfo-pick-active) {
    background: #f7f2e0;
  }
  .varinfo-pick-active {
    background: #ede4c4;
    border-color: #a8924f;
    cursor: default;
  }
  .year-conflict {
    padding: 0.6rem 0.8rem;
    background: #fff7e6;
    border-left: 3px solid #e8c184;
    border-radius: 3px;
    color: #4b3a14;
    font-size: 0.9rem;
  }
  .year-conflict-msg {
    margin: 0 0 0.4rem;
    line-height: 1.45;
  }
  .year-conflict-action {
    margin: 0;
    color: #5b4a14;
    line-height: 1.4;
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
  .value-codes-inline {
    border: 1px solid #e6dff2;
    background: #faf7fe;
    border-radius: 4px;
    padding: 0.35rem 0.6rem;
    font-size: 0.9rem;
  }
  .value-codes-inline > summary {
    cursor: pointer;
    user-select: none;
    color: #5d2b8c;
    font-size: 0.88rem;
  }
  .value-codes-inline[open] > summary {
    margin-bottom: 0.4rem;
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
    flex-wrap: wrap;
  }
  .unset {
    /* Destructive-flavoured but not primary: an orange tint signals
       "this drops your edit" without competing with the blue Save. */
    border-color: #d49a4f;
    color: #884a14;
    margin-right: auto;
  }
  .unset:hover:not(:disabled) {
    background: #fdf3e3;
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
