<script lang="ts">
  import { untrack } from "svelte";

  import { sourceYearInfoFor, store } from "../store.svelte";
  import type { RegisterGroupView } from "../types";
  import Modal from "./Modal.svelte";
  import RegisterCombobox from "./RegisterCombobox.svelte";

  interface Props {
    group: RegisterGroupView;
    onClose: () => void;
  }

  let { group, onClose }: Props = $props();

  // Modal dialog: snapshot the prop once on mount; `untrack` signals
  // that not reacting to upstream `group` changes is intentional.
  const initialName: string = untrack(() => group.register_name ?? "");
  const initialSources: readonly string[] = untrack(() => [...group.sources]);
  const isSingleton = initialSources.length === 1;
  let selectedRegister: string = $state(initialName);
  let reclassifyManual = $state(false);
  let submitting = $state(false);
  let confirming = $state(false);
  let validationError: string | null = $state(null);

  // Per-source inclusion. Unchecked sources fall out into their own
  // `noreg-<source_name>` group on the next snapshot rebuild. Default
  // every source to checked — Edit register must remain a no-op when
  // the user only opens the modal and clicks Apply.
  let sourceIncluded: Record<string, boolean> = $state(
    Object.fromEntries(initialSources.map((sn) => [sn, true])),
  );

  // Snapshot the year + provenance for each source at modal-open time.
  // The form's editable values diverge locally; on Save we diff and POST
  // only the sources whose value moved.
  const initialInfo = untrack(() => sourceYearInfoFor(initialSources));
  const initialYears: Record<string, number | null> = Object.fromEntries(
    initialSources.map((sn) => [sn, initialInfo[sn].year]),
  );
  const initialProvenance: Record<string, "set" | "missing"> = Object.fromEntries(
    initialSources.map((sn) => [sn, initialInfo[sn].provenance]),
  );

  // Editable per-source year input. Empty string = "unset" — clearing a
  // previously-set year persists as the `year` key being removed from
  // the config entirely, sending the row back to the "missing" state
  // (⚠ resurfaces on next read). Pre-filled from the stored value when
  // present; rows that are already missing start empty.
  let yearInput: Record<string, string> = $state(
    Object.fromEntries(
      initialSources.map((sn) => {
        if (initialProvenance[sn] === "missing") return [sn, ""];
        const y = initialYears[sn];
        return [sn, y === null ? "" : String(y)];
      }),
    ),
  );

  // Free-text year input with a `\d{4}` shape check. A floored dropdown
  // (e.g. 1990) was too narrow — Folk- och bostadsräkningen reaches back
  // to 1960 and individual SCB registers go back to 1968 or earlier. We
  // don't have a credible upper bound either (delivered years can run
  // ahead of the calendar year). Trust the user; the server validates
  // the int shape independently.
  const YEAR_RE = /^\d{4}$/;

  // Parse a year input string into the value we'll POST. Empty string
  // means "no year" (null). Returns `undefined` for invalid input
  // (anything that isn't four digits) so the caller can flag it
  // without conflating with intentional null.
  function parseYearInput(v: string): number | null | undefined {
    const trimmed = v.trim();
    if (trimmed === "") return null;
    if (!YEAR_RE.test(trimmed)) return undefined;
    return Number(trimmed);
  }

  // Year changes the user wants to persist. Three intents land here:
  //   1. "set" row with a different valid year → persist new int
  //   2. "missing" row with a typed year → persist new int
  //   3. "set" row cleared to empty → persist null (server interprets
  //      as "delete the year key", sending the row back to "missing")
  // A "missing" row left empty is a no-op.
  let yearAssignments: Record<string, number | null> = $derived.by(() => {
    const out: Record<string, number | null> = {};
    for (const sn of initialSources) {
      const wasSet = initialProvenance[sn] === "set";
      const current = wasSet ? initialYears[sn] : undefined;
      const parsed = parseYearInput(yearInput[sn] ?? "");
      if (parsed === undefined) continue;
      if (wasSet && parsed === current) continue;
      // No-op: a missing row left empty is the user not touching the
      // field at all.
      if (!wasSet && parsed === null) continue;
      out[sn] = parsed;
    }
    return out;
  });

  let yearHasInvalid = $derived(
    Object.values(yearInput).some(
      (v) => parseYearInput(v) === undefined,
    ),
  );

  let yearChangedCount = $derived(Object.keys(yearAssignments).length);
  let yearMissingCount = $derived(
    initialSources.filter((sn) => initialProvenance[sn] === "missing").length,
  );

  $effect(() => {
    void store.ensureRegisters();
  });

  let registers = $derived(store.registers ?? []);
  // When the register list couldn't load (reg_meta unavailable), the
  // client-side gate has nothing to validate against. Skip it so manual
  // entry still works; the server-side validator stays the source of
  // truth. The user gets a warning toast on the failed fetch, so the
  // missing autocomplete isn't silent.
  let canValidateLocally = $derived(
    !store.registersUnavailable && registers.length > 0,
  );

  // Mirror the server's resolve order (reg_meta.queries.resolve_register_ids):
  //   1. exact numeric register_id
  //   2. case-insensitive exact name
  //   3. case-insensitive substring (only if uniquely resolves)
  // The client previously only matched display names, which blocked
  // valid numeric IDs and unique substrings the backend would accept.
  function resolvesAgainstList(input: string): boolean {
    const v = input.trim();
    if (!v) return true;
    const asNum = Number(v);
    if (Number.isInteger(asNum) && registers.some((r) => r.id === asNum)) {
      return true;
    }
    const lower = v.toLowerCase();
    if (registers.some((r) => r.name.toLowerCase() === lower)) return true;
    const matches = registers.filter((r) =>
      r.name.toLowerCase().includes(lower),
    );
    return matches.length === 1;
  }

  let trimmedRegister = $derived(selectedRegister.trim());
  let registerChanged = $derived(trimmedRegister !== initialName);
  let inputResolves = $derived(
    !canValidateLocally || resolvesAgainstList(trimmedRegister),
  );

  // Buckets of sources by user intent. Excluded sources go to their own
  // noreg group; included sources either keep or pick up the (possibly
  // new) register. Singleton groups never enter the exclusion bucket —
  // there's nothing to peel off.
  let includedSources = $derived(
    isSingleton
      ? initialSources.slice()
      : initialSources.filter((sn) => sourceIncluded[sn]),
  );
  let excludedSources = $derived(
    isSingleton ? [] : initialSources.filter((sn) => !sourceIncluded[sn]),
  );
  let allExcluded = $derived(
    !isSingleton && excludedSources.length === initialSources.length,
  );

  // Manuals across the whole group — keeps the reclassify_manual
  // checkbox reachable even when nothing else has changed, so a user
  // can drop accidental manual edits without flipping the register.
  let groupManualCount = $derived.by(() => {
    const manual = store.snapshot?.config.manual_columns ?? [];
    const sources = new Set(initialSources);
    return manual.filter(([s, _c]) => sources.has(s)).length;
  });

  let intendsReclassify = $derived(reclassifyManual && groupManualCount > 0);

  // Sources whose state would actually move. Excluded sources always
  // move (register cleared); included sources move on a register change
  // or when reclassify_manual forces a re-run on them.
  let affectedSources = $derived.by(() => {
    const out: string[] = [];
    for (const sn of excludedSources) out.push(sn);
    if (registerChanged || intendsReclassify) {
      for (const sn of includedSources) out.push(sn);
    }
    return out;
  });

  // Manuals on the actually-affected source set. Drives the register-
  // change confirm branch so partial exclusion of clean sources doesn't
  // over-trip the warning.
  let affectedManualCount = $derived.by(() => {
    const manual = store.snapshot?.config.manual_columns ?? [];
    const affected = new Set(affectedSources);
    return manual.filter(([s, _c]) => affected.has(s)).length;
  });

  // Panel membership per source — used to surface the "still in panel"
  // hint next to each unchecked source. Panels live independently of
  // register, so excluding a source doesn't drop its panel slot.
  let panelsBySource = $derived.by(() => {
    const out: Record<string, string[]> = {};
    const panels = store.snapshot?.config.panels ?? [];
    for (const panel of panels) {
      for (const m of panel.members) {
        if (!out[m.source]) out[m.source] = [];
        out[m.source].push(panel.panel_id);
      }
    }
    return out;
  });

  let hasExclusions = $derived(excludedSources.length > 0);
  // Year saves and register saves go to different endpoints. A year-only
  // edit must not be blocked by an unresolved register field — the
  // register isn't being written in that path, so its validity is moot.
  let registerNeedsWrite = $derived(
    registerChanged || intendsReclassify || hasExclusions,
  );
  let canApply = $derived(
    (registerNeedsWrite || yearChangedCount > 0) &&
      (!registerNeedsWrite || inputResolves) &&
      !yearHasInvalid &&
      !submitting,
  );
  // Confirm fires on destructive reclassification — manuals at risk or
  // explicit reclassify-manuals — or on full clear (all sources
  // unchecked), which is structurally equivalent to clearing the
  // register on the group.
  let needsConfirm = $derived(
    (registerChanged && affectedManualCount > 0) ||
      intendsReclassify ||
      allExcluded,
  );

  function valueOrNull(): string | null {
    return trimmedRegister === "" ? null : trimmedRegister;
  }

  function buildAssignments(finalValue: string | null): Record<string, string | null> {
    const out: Record<string, string | null> = {};
    // Excluded sources → cleared.
    for (const sn of excludedSources) out[sn] = null;
    // Included sources → the final register value (may equal current).
    for (const sn of includedSources) out[sn] = finalValue;
    return out;
  }

  async function commit(): Promise<void> {
    const version = store.snapshot?.snapshot_version;
    if (!version) return;
    // Capture values that drive the success toast before the API call:
    // the snapshot refresh during `await` re-derives manualCount and
    // bucket sizes, which would otherwise pick the wrong toast branch.
    const finalValue = valueOrNull();
    const wasReclassifyOnly =
      !registerChanged && !hasExclusions && intendsReclassify;
    // Reclassify-only forces a re-run on every included source, so the
    // toast count is the whole group's manuals (no exclusions in this
    // branch). `affectedManualCount` works too but `groupManualCount`
    // is the clearer signal of "what got dropped".
    const reclassifiedCount = groupManualCount;
    const includedCount = includedSources.length;
    const excludedCount = excludedSources.length;
    const wasFullClear = allExcluded || finalValue === null;
    const targetName = initialName !== "" ? initialName : (finalValue ?? "");
    const yearChanges = { ...yearAssignments };
    const yearChangeCount = Object.keys(yearChanges).length;

    submitting = true;
    let ok = true;
    if (registerNeedsWrite) {
      ok = await store.setSourceRegisters({
        assignments: buildAssignments(finalValue),
        expected_version: version,
        reclassify_manual: reclassifyManual,
      });
    }
    // Years are persisted after register changes (whose write bumps the
    // snapshot version). We pull the fresh version from the store
    // between calls — this is not transactional across the two
    // endpoints, but the second call validates independently so partial
    // failure leaves the register write in place rather than rolling it
    // back. Tradeoff documented; the alternative is widening
    // set_source_registers' contract, which would force every
    // register-only call to ship year info too.
    if (ok && yearChangeCount > 0) {
      const next = store.snapshot?.snapshot_version;
      if (next) {
        ok = await store.setSourceYears({
          assignments: yearChanges,
          expected_version: next,
        });
      } else {
        ok = false;
      }
    }
    submitting = false;
    if (ok) {
      let message: string;
      if (yearChangeCount > 0 && !registerNeedsWrite) {
        message =
          yearChangeCount === 1
            ? `Updated year on 1 source`
            : `Updated year on ${yearChangeCount} sources`;
      } else if (wasReclassifyOnly) {
        message = `Re-classified ${reclassifiedCount} manual column${reclassifiedCount === 1 ? "" : "s"} on ${group.group_id}`;
      } else if (wasFullClear) {
        message = `Cleared register on ${group.group_id}`;
      } else if (registerChanged && excludedCount > 0) {
        message = `Set register on ${includedCount}, excluded ${excludedCount}`;
      } else if (excludedCount > 0) {
        message = `Excluded ${excludedCount} source${excludedCount === 1 ? "" : "s"} from ${targetName}`;
      } else if (finalValue !== null) {
        message = `Set ${group.group_id} → ${finalValue}`;
      } else {
        message = `Cleared register on ${group.group_id}`;
      }
      if (yearChangeCount > 0 && registerNeedsWrite) {
        message += ` · year on ${yearChangeCount} source${yearChangeCount === 1 ? "" : "s"}`;
      }
      store.pushToast("info", message);
      onClose();
    }
  }

  async function onSubmit(event: SubmitEvent): Promise<void> {
    event.preventDefault();
    if (submitting) return;
    if (!canApply) return; // no-op guard
    // Only validate the register field when this submit will actually
    // write the register. Year-only saves go to a separate endpoint and
    // don't read the field at all.
    if (registerNeedsWrite && !inputResolves) {
      validationError = `'${trimmedRegister}' is not a known register name. Pick one from the autocomplete or leave the field empty to clear the assignment.`;
      return;
    }
    if (needsConfirm && !confirming) {
      confirming = true;
      return;
    }
    await commit();
  }

  function onInput() {
    // Clear inline error eagerly — the user is typing again.
    validationError = null;
    // Stepping back through Apply→Confirm should drop the confirmation
    // if the user changes the register again mid-flow.
    confirming = false;
  }

  function toggleSource(sn: string): void {
    sourceIncluded[sn] = !sourceIncluded[sn];
    confirming = false;
  }

  // When the user clears the register input, the per-source checkboxes
  // become meaningless — every source ends up cleared either way. Lock
  // them so the modal doesn't show contradictory affordances.
  let checkboxesDisabled = $derived(trimmedRegister === "");
</script>

<Modal headingId="register-editor-heading" {onClose}>
  <form onsubmit={onSubmit}>
    <header>
      <h3 id="register-editor-heading">
        Set register · <span class="mono">{group.group_id}</span>
      </h3>
      <button type="button" class="close" aria-label="Close" onclick={onClose}>
        ×
      </button>
    </header>

    <div class="modal-body">
      <label class="row">
        <span>Register</span>
        <RegisterCombobox
          {registers}
          bind:value={selectedRegister}
          oninput={onInput}
          ariaInvalid={!inputResolves}
          ariaDescribedby={validationError ? "register-error" : undefined}
        />
      </label>

      {#if validationError}
        <p id="register-error" class="error" role="alert">{validationError}</p>
      {:else if !inputResolves && trimmedRegister !== "" && registerNeedsWrite}
        <p class="hint-line">
          not a known register — Apply will be blocked until you pick one
          from the suggestions.
        </p>
      {/if}

      {#if !isSingleton}
        <fieldset class="sources">
          <legend>
            Sources in this group
            <span class="count">
              ({includedSources.length} / {initialSources.length} included)
            </span>
          </legend>
          <ul>
            {#each initialSources as src (src)}
              {@const included = sourceIncluded[src]}
              {@const panels = panelsBySource[src] ?? []}
              <li class:excluded={!included}>
                <label>
                  <input
                    type="checkbox"
                    checked={included}
                    disabled={checkboxesDisabled || submitting}
                    onchange={() => toggleSource(src)}
                  />
                  <span class="mono">{src}</span>
                </label>
                {#if !included && !checkboxesDisabled}
                  <span class="exclusion-hint">
                    → will become its own group
                    {#if panels.length > 0}
                      <span class="panel-note">
                        · still in panel{panels.length === 1 ? "" : "s"}
                        <span class="mono">{panels.join(", ")}</span>
                      </span>
                    {/if}
                  </span>
                {/if}
              </li>
            {/each}
          </ul>
        </fieldset>
      {/if}

      <fieldset class="years">
        <legend>
          Source year{initialSources.length === 1 ? "" : "s"}
          {#if yearMissingCount > 0}
            <span class="legend-warn" title="auto-detection failed for these sources">
              · ⚠ {yearMissingCount} missing
            </span>
          {/if}
        </legend>
        <p class="hint-line years-hint">
          Year scopes variable descriptions, value codes, and CVID picking
          to the right register version. Rows marked ⚠ had no year
          detected from the source name; type a 4-digit year to fix.
        </p>
        <ul class="year-rows">
          {#each initialSources as src (src)}
            {@const prov = initialProvenance[src]}
            {@const value = yearInput[src] ?? ""}
            {@const parsed = parseYearInput(value)}
            {@const invalid = parsed === undefined}
            <li class="year-row" class:row-missing={prov === "missing"}>
              <span class="year-source mono" title={src}>{src}</span>
              <span class="year-controls">
                {#if prov === "missing"}
                  <span class="year-warn" title="no year detected from source name — type a 4-digit year">
                    ⚠
                  </span>
                {/if}
                <input
                  type="text"
                  inputmode="numeric"
                  maxlength="4"
                  placeholder="YYYY"
                  class="year-input"
                  class:year-input-invalid={invalid}
                  bind:value={yearInput[src]}
                  oninput={() => (confirming = false)}
                  disabled={submitting}
                  aria-label={`year for ${src}`}
                  aria-invalid={invalid}
                  spellcheck="false"
                />
              </span>
            </li>
          {/each}
        </ul>
      </fieldset>

      {#if groupManualCount > 0}
        <label class="checkbox">
          <input
            type="checkbox"
            bind:checked={reclassifyManual}
            onchange={() => (confirming = false)}
          />
          Re-classify the {groupManualCount} manually-edited column{groupManualCount === 1 ? "" : "s"}
        </label>
      {/if}

      {#if confirming}
        <p class="warn">
          {#if allExcluded}
            All sources unchecked — equivalent to clearing the register
            on the group. Each source will become its own
            <span class="mono">noreg-…</span> group.
          {:else}
            This will re-run classification on
            {#if reclassifyManual}all{:else}auto-classified{/if}
            columns in {affectedSources.length} source{affectedSources.length === 1
              ? ""
              : "s"}. Manual columns are
            {reclassifyManual ? "included" : "preserved"}.
          {/if}
        </p>
      {/if}
    </div>

    <footer>
      <button type="button" onclick={onClose} disabled={submitting}
        >Cancel</button
      >
      <button
        type="submit"
        class="primary"
        disabled={!canApply}
        title={!canApply && !submitting
          ? !inputResolves
            ? "Pick a known register"
            : "No change to apply"
          : ""}
      >
        {#if submitting}
          Saving…
        {:else if confirming}
          Confirm
        {:else}
          Apply
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
  h3 {
    margin: 0;
    font-size: 1rem;
    min-width: 0;
    flex: 1 1 auto;
    word-break: break-word;
  }
  .mono {
    font-family: ui-monospace, monospace;
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
  .row {
    display: grid;
    grid-template-columns: 6rem 1fr;
    align-items: center;
    gap: 0.5rem;
  }
  .row span {
    color: #666;
    font-size: 0.9rem;
  }
  .checkbox {
    display: flex;
    align-items: center;
    gap: 0.4rem;
    color: #333;
  }
  .sources {
    border: 1px solid #ddd;
    border-radius: 4px;
    padding: 0.5rem 0.75rem;
    margin: 0;
    min-width: 0;
  }
  .sources legend {
    padding: 0 0.4rem;
    font-size: 0.85rem;
    color: #555;
  }
  .sources legend .count {
    color: #888;
  }
  .sources ul {
    list-style: none;
    padding: 0;
    margin: 0;
    /* Long source lists scroll; modal header/footer stay pinned. */
    max-height: 14rem;
    overflow-y: auto;
  }
  .sources li {
    display: flex;
    flex-wrap: wrap;
    align-items: baseline;
    gap: 0.5rem;
    padding: 0.15rem 0;
    font-size: 0.9rem;
  }
  .sources li label {
    display: inline-flex;
    align-items: center;
    gap: 0.4rem;
    cursor: pointer;
    min-width: 0;
    word-break: break-all;
  }
  .sources li.excluded label .mono {
    text-decoration: line-through;
    color: #888;
  }
  .exclusion-hint {
    color: #888;
    font-size: 0.85rem;
  }
  .panel-note {
    color: #b86c00;
  }
  .years {
    border: 1px solid #ddd;
    border-radius: 4px;
    padding: 0.5rem 0.75rem;
    margin: 0;
    min-width: 0;
  }
  .years legend {
    padding: 0 0.4rem;
    font-size: 0.85rem;
    color: #555;
  }
  .legend-warn {
    color: #7a4a00;
    font-weight: 500;
  }
  .years-hint {
    margin: 0 0 0.4rem;
    line-height: 1.35;
  }
  .year-rows {
    list-style: none;
    padding: 0;
    margin: 0;
    max-height: 16rem;
    overflow-y: auto;
    display: flex;
    flex-direction: column;
    gap: 0.2rem;
  }
  .year-row {
    display: grid;
    grid-template-columns: 1fr auto;
    gap: 0.5rem;
    align-items: center;
    padding: 0.2rem 0;
    font-size: 0.88rem;
  }
  .year-row.row-missing {
    background: #fff7e6;
    border-radius: 3px;
    padding: 0.2rem 0.4rem;
  }
  .year-source {
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    min-width: 0;
  }
  .year-controls {
    display: inline-flex;
    align-items: center;
    gap: 0.3rem;
  }
  .year-warn {
    color: #7a4a00;
    font-size: 0.95em;
    cursor: help;
  }
  .year-input {
    width: 5.5rem;
    padding: 0.25rem 0.4rem;
    border: 1px solid #ccc;
    border-radius: 3px;
    font: inherit;
    font-family: ui-monospace, monospace;
    text-align: center;
  }
  .year-input-invalid {
    border-color: #d49a4f;
    background: #fdf3e3;
  }
  .warn {
    background: #fff8e1;
    border: 1px solid #f0c14b;
    border-radius: 4px;
    padding: 0.5rem 0.75rem;
    margin: 0;
    color: #5b4a14;
    font-size: 0.9rem;
  }
  .error {
    background: #fde8e8;
    border: 1px solid #e0a0a0;
    border-radius: 4px;
    padding: 0.5rem 0.75rem;
    margin: 0;
    color: #882020;
    font-size: 0.88rem;
  }
  .hint-line {
    margin: 0;
    color: #888;
    font-size: 0.85rem;
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
