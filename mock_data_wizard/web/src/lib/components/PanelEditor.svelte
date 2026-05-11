<script lang="ts">
  import { store } from "../store.svelte";
  import type {
    ColumnInfo,
    Panel,
    PanelMember,
    RegisterGroupView,
  } from "../types";
  import Modal from "./Modal.svelte";

  interface Props {
    group: RegisterGroupView;
    /** The currently-attached panel for this group, if any. Resolved by
     *  the caller from snapshot.config.panels via member-source overlap
     *  with the group's sources. */
    existing: Panel | null;
    onClose: () => void;
  }

  let { group, existing, onClose }: Props = $props();

  type TimeKeyMode = "name" | "column";
  interface MemberDraft {
    selected: boolean;
    /** "name" = literal int period from the source name. "column" = a
     *  string column name on the source whose values are the period. */
    mode: TimeKeyMode;
    /** Free-text representation of the literal period (so we can keep
     *  the field controllable even before the user types a digit). */
    nameValue: string;
    /** Selected column name when mode = "column". Empty string means
     *  "no column picked yet" — validation surfaces that as an error. */
    columnValue: string;
  }

  // -- Helpers (mirror panels.py's auto-detection) ---------------------

  // Naive 4-digit year search. Matches `detect_year_from_source_name`
  // on the Python side.
  const YEAR_RE = /\d{4}/;
  function detectYearInName(name: string): number | null {
    const m = name.match(YEAR_RE);
    return m ? parseInt(m[0], 10) : null;
  }

  // Same lowercase set as panels._TIME_KEY_NAMES.
  const TIME_KEY_NAMES = new Set(["ar", "indatum", "year", "period"]);
  function detectTimeKeyColumn(columns: readonly ColumnInfo[]): string | null {
    for (const c of columns) {
      if (TIME_KEY_NAMES.has(c.name.toLowerCase())) return c.name;
    }
    return null;
  }

  // -- Initial draft from existing / candidate / auto-detect ----------

  let allSources: readonly string[] = $derived(group.sources);

  function buildDraft(): Record<string, MemberDraft> {
    const fromExisting = new Map<string, number | string>();
    if (existing) {
      for (const m of existing.members) fromExisting.set(m.source, m.time_key);
    }
    const fromCandidate = new Map<string, number | string>();
    if (!existing && group.panel_candidate) {
      for (const m of group.panel_candidate.members) {
        fromCandidate.set(m.source, m.time_key);
      }
    }
    const seeded = fromExisting.size > 0 ? fromExisting : fromCandidate;

    const out: Record<string, MemberDraft> = {};
    for (const sn of allSources) {
      const cols = group.columns_by_source[sn] ?? [];
      const seededKey = seeded.get(sn);
      const yearFromName = detectYearInName(sn);
      const detectedColumn = detectTimeKeyColumn(cols);

      let mode: TimeKeyMode;
      let nameValue: string;
      let columnValue: string;

      if (typeof seededKey === "number") {
        mode = "name";
        nameValue = String(seededKey);
        columnValue = detectedColumn ?? "";
      } else if (typeof seededKey === "string") {
        mode = "column";
        nameValue = yearFromName !== null ? String(yearFromName) : "";
        columnValue = seededKey;
      } else if (yearFromName !== null) {
        mode = "name";
        nameValue = String(yearFromName);
        columnValue = detectedColumn ?? "";
      } else if (detectedColumn !== null) {
        mode = "column";
        nameValue = "";
        columnValue = detectedColumn;
      } else {
        // Nothing detected — leave both blank; user can pick a mode.
        mode = "name";
        nameValue = "";
        columnValue = "";
      }

      // Pre-checked if seeded by existing/candidate. Otherwise pre-check
      // whenever the auto-detector found a hint; sources with no signal
      // are unchecked so the user has to opt them in explicitly.
      const selected =
        seeded.size > 0
          ? seeded.has(sn)
          : yearFromName !== null || detectedColumn !== null;

      out[sn] = { selected, mode, nameValue, columnValue };
    }
    return out;
  }

  let draft: Record<string, MemberDraft> = $state(buildDraft());

  function defaultPanelId(): string {
    if (existing) return existing.panel_id;
    if (group.panel_candidate?.suggested_panel_id) {
      return group.panel_candidate.suggested_panel_id;
    }
    return group.register_name ?? group.group_id;
  }
  function defaultEntityKey(): string {
    if (existing) return existing.entity_key;
    return group.panel_candidate?.suggested_entity_key ?? "";
  }

  let panelId: string = $state(defaultPanelId());
  let entityKey: string = $state(defaultEntityKey());
  let submitting = $state(false);
  let confirmingRemoval = $state(false);

  // -- Derived state --------------------------------------------------

  let selectedSources: string[] = $derived(
    allSources.filter((sn) => draft[sn]?.selected),
  );

  // Columns common to every selected source — these are the valid
  // entity_key candidates. Empty when nothing is selected.
  let entityKeyOptions: string[] = $derived.by(() => {
    if (selectedSources.length === 0) return [];
    const sets = selectedSources.map(
      (sn) =>
        new Set((group.columns_by_source[sn] ?? []).map((c) => c.name)),
    );
    const [first, ...rest] = sets;
    const intersection = new Set<string>();
    for (const name of first) {
      if (rest.every((s) => s.has(name))) intersection.add(name);
    }
    return [...intersection].sort();
  });

  // Build the resolved member list, plus per-source validation messages.
  interface BuiltMember {
    source: string;
    member: PanelMember | null;
    error: string | null;
  }
  let builtMembers: BuiltMember[] = $derived.by(() => {
    return selectedSources.map((sn) => {
      const d = draft[sn];
      if (d.mode === "name") {
        const raw = d.nameValue.trim();
        if (raw === "") {
          return {
            source: sn,
            member: null,
            error: "literal period required",
          };
        }
        const n = Number(raw);
        if (!Number.isInteger(n)) {
          return {
            source: sn,
            member: null,
            error: "literal period must be an integer",
          };
        }
        return { source: sn, member: { source: sn, time_key: n }, error: null };
      }
      const col = d.columnValue;
      if (!col) {
        return { source: sn, member: null, error: "pick a column" };
      }
      const cols = group.columns_by_source[sn] ?? [];
      if (!cols.some((c) => c.name === col)) {
        return {
          source: sn,
          member: null,
          error: `column '${col}' missing on this source`,
        };
      }
      return {
        source: sn,
        member: { source: sn, time_key: col },
        error: null,
      };
    });
  });

  let memberErrors: string[] = $derived(
    builtMembers.filter((b) => b.error !== null).map((b) => `${b.source}: ${b.error}`),
  );

  // Reject duplicate integer time_keys across file-members (matches the
  // server's parse_panel check). Column time_keys can repeat — they
  // materialise at extract time.
  let duplicateLiteralError: string | null = $derived.by(() => {
    const seen = new Set<number>();
    for (const b of builtMembers) {
      if (b.member && typeof b.member.time_key === "number") {
        if (seen.has(b.member.time_key)) {
          return `duplicate literal period ${b.member.time_key}`;
        }
        seen.add(b.member.time_key);
      }
    }
    return null;
  });

  let entityKeyError: string | null = $derived.by(() => {
    const trimmed = entityKey.trim();
    if (!trimmed) return "entity_key required";
    if (selectedSources.length === 0) return null;
    for (const sn of selectedSources) {
      const cols = group.columns_by_source[sn] ?? [];
      if (!cols.some((c) => c.name === trimmed)) {
        return `column '${trimmed}' missing on ${sn}`;
      }
    }
    return null;
  });

  let panelIdTrimmed = $derived(panelId.trim());
  let entityKeyTrimmed = $derived(entityKey.trim());
  let validationErrors: string[] = $derived(
    [
      selectedSources.length === 0 ? "select at least one source" : null,
      panelIdTrimmed.length === 0 ? "panel_id required" : null,
      entityKeyError,
      duplicateLiteralError,
      ...memberErrors,
    ].filter((m): m is string => m !== null),
  );

  let canSave = $derived(!submitting && validationErrors.length === 0);

  function resetConfirm(): void {
    confirmingRemoval = false;
  }

  function setMode(sn: string, mode: TimeKeyMode): void {
    draft[sn] = { ...draft[sn], mode };
    resetConfirm();
  }

  function toggleSource(sn: string): void {
    draft[sn] = { ...draft[sn], selected: !draft[sn].selected };
    resetConfirm();
  }

  async function onSubmit(event: SubmitEvent): Promise<void> {
    event.preventDefault();
    if (!canSave) return;
    const version = store.snapshot?.snapshot_version;
    if (!version) return;
    const members: PanelMember[] = builtMembers
      .map((b) => b.member)
      .filter((m): m is PanelMember => m !== null);
    submitting = true;
    const args = {
      panel_id: panelIdTrimmed,
      entity_key: entityKeyTrimmed,
      members,
      expected_version: version,
      // Pass the renamed-from id when the panel_id changed; server drops
      // the old entry in the same lock so source-overlap doesn't reject
      // the rename. Same value as new id is a no-op.
      ...(existing && existing.panel_id !== panelIdTrimmed
        ? { previous_panel_id: existing.panel_id }
        : {}),
    };
    const ok = await store.putPanel(args);
    submitting = false;
    if (ok) {
      store.pushToast(
        "info",
        existing
          ? `Updated panel ${args.panel_id}`
          : `Created panel ${args.panel_id} (${args.members.length} members)`,
      );
      onClose();
    }
  }

  async function onRemove(): Promise<void> {
    if (!existing) return;
    if (!confirmingRemoval) {
      confirmingRemoval = true;
      return;
    }
    const version = store.snapshot?.snapshot_version;
    if (!version) return;
    submitting = true;
    const removedId = existing.panel_id;
    const ok = await store.removePanel({
      panel_id: removedId,
      expected_version: version,
    });
    submitting = false;
    if (ok) {
      store.pushToast("info", `Removed panel ${removedId}`);
      onClose();
    }
  }

  // Single-source groups skip the source picker entirely — there's
  // nothing to pick from. The lone source is always implicitly selected.
  let isSingletonGroup = $derived(allSources.length === 1);
</script>

<Modal headingId="panel-editor-heading" {onClose}>
  <form onsubmit={onSubmit}>
    <header>
      <div class="heading-stack">
        <span class="meta-line">
          panel · <span class="mono">{group.group_id}</span>
        </span>
        <h3 id="panel-editor-heading">
          {existing ? "Edit panel" : "Designate panel"}
        </h3>
      </div>
      <button type="button" class="close" aria-label="Close" onclick={onClose}>
        ×
      </button>
    </header>

    <label class="row">
      <span>panel_id</span>
      <input
        type="text"
        bind:value={panelId}
        spellcheck="false"
        onfocus={resetConfirm}
        oninput={resetConfirm}
        placeholder={existing
          ? existing.panel_id
          : group.panel_candidate?.suggested_panel_id ?? ""}
      />
    </label>

    <label class="row">
      <span>entity_key</span>
      {#if entityKeyOptions.length > 0}
        <select
          bind:value={entityKey}
          onfocus={resetConfirm}
          onchange={resetConfirm}
        >
          {#if entityKey && !entityKeyOptions.includes(entityKey)}
            <option value={entityKey} disabled>{entityKey} (missing on a member)</option>
          {/if}
          {#if entityKey === ""}
            <option value="" disabled>— pick a column —</option>
          {/if}
          {#each entityKeyOptions as col (col)}
            <option value={col}>{col}</option>
          {/each}
        </select>
      {:else}
        <input
          type="text"
          bind:value={entityKey}
          spellcheck="false"
          onfocus={resetConfirm}
          oninput={resetConfirm}
          placeholder="id column shared across members"
        />
      {/if}
    </label>

    <fieldset class="members">
      <legend>
        Members ({selectedSources.length}/{allSources.length})
      </legend>
      <ul>
        {#each allSources as sn (sn)}
          {@const d = draft[sn]}
          {@const cols = group.columns_by_source[sn] ?? []}
          <li class:disabled={!d.selected}>
            <label class="source-row">
              {#if !isSingletonGroup}
                <input
                  type="checkbox"
                  checked={d.selected}
                  onchange={() => toggleSource(sn)}
                />
              {/if}
              <span class="mono source-name">{sn}</span>
            </label>
            {#if d.selected}
              <div class="time-key-block">
                <div class="mode-toggle" role="radiogroup" aria-label="time_key source">
                  <label class:active={d.mode === "name"}>
                    <input
                      type="radio"
                      name={`mode-${sn}`}
                      value="name"
                      checked={d.mode === "name"}
                      onchange={() => setMode(sn, "name")}
                    />
                    From source name
                  </label>
                  <label class:active={d.mode === "column"}>
                    <input
                      type="radio"
                      name={`mode-${sn}`}
                      value="column"
                      checked={d.mode === "column"}
                      onchange={() => setMode(sn, "column")}
                    />
                    From column
                  </label>
                </div>
                {#if d.mode === "name"}
                  <input
                    type="number"
                    step="1"
                    bind:value={d.nameValue}
                    onfocus={resetConfirm}
                    oninput={resetConfirm}
                    placeholder="literal period (e.g. 2018)"
                    aria-label={`time_key literal for ${sn}`}
                  />
                {:else}
                  <select
                    bind:value={d.columnValue}
                    onfocus={resetConfirm}
                    onchange={resetConfirm}
                    aria-label={`time_key column for ${sn}`}
                  >
                    {#if d.columnValue === ""}
                      <option value="" disabled>— pick a column —</option>
                    {/if}
                    {#each cols as c (c.name)}
                      <option value={c.name}>{c.name}</option>
                    {/each}
                  </select>
                {/if}
              </div>
            {/if}
          </li>
        {/each}
      </ul>
    </fieldset>

    {#if validationErrors.length > 0 && !canSave}
      <ul class="errors" aria-live="polite">
        {#each validationErrors as msg (msg)}
          <li>{msg}</li>
        {/each}
      </ul>
    {/if}

    <footer>
      {#if existing}
        <button
          type="button"
          class="remove"
          class:armed={confirmingRemoval}
          onclick={onRemove}
          disabled={submitting}
          title={confirmingRemoval
            ? "Click again to confirm removal"
            : "Remove this panel from the project"}
        >
          {confirmingRemoval ? "Confirm remove" : "Remove panel"}
        </button>
      {/if}
      <button
        type="button"
        onclick={() => {
          resetConfirm();
          onClose();
        }}
        disabled={submitting}
      >
        Cancel
      </button>
      <button type="submit" class="primary" disabled={!canSave}>
        {#if submitting}
          Saving…
        {:else if existing}
          Save
        {:else}
          Designate
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
  .meta-line {
    color: #777;
    font-size: 0.82rem;
  }
  h3 {
    margin: 0;
    font-size: 1.05rem;
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
    grid-template-columns: 7rem 1fr;
    align-items: center;
    gap: 0.5rem;
  }
  .row span {
    color: #666;
    font-size: 0.9rem;
  }
  input[type="text"],
  input[type="number"],
  select {
    padding: 0.3rem 0.5rem;
    border: 1px solid #ccc;
    border-radius: 4px;
    font: inherit;
    font-family: ui-monospace, monospace;
    width: 100%;
    box-sizing: border-box;
  }
  .members {
    border: 1px solid #eee;
    border-radius: 4px;
    padding: 0.5rem 0.75rem 0.75rem;
    background: #fafbff;
    margin: 0;
  }
  .members legend {
    color: #666;
    font-size: 0.85rem;
    padding: 0 0.3rem;
  }
  .members ul {
    list-style: none;
    margin: 0;
    padding: 0;
    display: flex;
    flex-direction: column;
    gap: 0.4rem;
  }
  .members li {
    padding: 0.35rem 0.45rem;
    border: 1px solid #eef0f5;
    border-radius: 4px;
    background: #fff;
    display: flex;
    flex-direction: column;
    gap: 0.4rem;
  }
  .members li.disabled {
    background: #f6f6f6;
    color: #888;
  }
  .source-row {
    display: flex;
    align-items: center;
    gap: 0.5rem;
    cursor: pointer;
  }
  .source-name {
    flex: 1 1 auto;
    overflow-wrap: anywhere;
    font-size: 0.92rem;
  }
  .time-key-block {
    display: flex;
    flex-direction: column;
    gap: 0.4rem;
    padding-left: 1.7rem;
  }
  .mode-toggle {
    display: inline-flex;
    gap: 0.25rem;
  }
  .mode-toggle label {
    cursor: pointer;
    padding: 0.15rem 0.5rem;
    border: 1px solid #ccc;
    border-radius: 3px;
    font-size: 0.82rem;
    color: #555;
    background: #fff;
    user-select: none;
  }
  .mode-toggle label.active {
    background: #1656c0;
    border-color: #1656c0;
    color: #fff;
  }
  .mode-toggle input {
    display: none;
  }
  .errors {
    background: #fff5f5;
    border: 1px solid #e0b4b4;
    border-radius: 4px;
    padding: 0.45rem 0.75rem 0.45rem 1.7rem;
    margin: 0;
    color: #7a2929;
    font-size: 0.85rem;
  }
  .errors li {
    margin: 0.1rem 0;
  }
  footer {
    display: flex;
    justify-content: flex-end;
    gap: 0.5rem;
    flex-wrap: wrap;
  }
  .remove {
    border-color: #d49a4f;
    color: #884a14;
    margin-right: auto;
  }
  .remove:hover:not(:disabled) {
    background: #fdf3e3;
  }
  /* Armed (one click already received) — flips to a danger appearance
     so the second click is clearly destructive, not a stray re-press
     of the original button. */
  .remove.armed {
    background: #b94a14;
    border-color: #b94a14;
    color: #fff;
  }
  .remove.armed:hover:not(:disabled) {
    background: #a23d10;
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
