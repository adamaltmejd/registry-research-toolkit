<script lang="ts">
  import { store } from "../store.svelte";
  import type { Panel, PanelMember, RegisterGroupView } from "../types";
  import HoverHint from "./HoverHint.svelte";
  import Modal from "./Modal.svelte";

  interface Props {
    group: RegisterGroupView;
    /** The currently-attached panel for this group, if any. Resolved by
     *  the caller from snapshot.config.panels via member-source overlap
     *  with the group's sources. */
    existing: Panel | null;
    /** When set (opening the picker from the "unassigned sources" box),
     *  only these sources are pre-selected and `group.panel_candidate`
     *  is ignored — the candidate is computed against the whole group
     *  and would pre-fill sources already claimed by another panel. */
    restrictToSources?: readonly string[];
    onClose: () => void;
  }

  let { group, existing, restrictToSources, onClose }: Props = $props();

  type TimeKeyMode = "name" | "column";
  const MODE_OPTIONS: readonly { value: TimeKeyMode; label: string }[] = [
    { value: "name", label: "From source name" },
    { value: "column", label: "From column" },
  ];
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

  let allSources: readonly string[] = $derived(group.sources);

  function buildDraft(): Record<string, MemberDraft> {
    const fromExisting = new Map<string, number | string>();
    if (existing) {
      for (const m of existing.members) fromExisting.set(m.source, m.time_key);
    }
    // `restrictToSources: []` is treated the same as omitting it: an empty
    // scope would otherwise leave the picker with nothing selected, which
    // is a footgun if a future caller forwards a possibly-empty leftover
    // list. Arrays are truthy in JS, so we have to check length explicitly.
    const hasRestrict = !!restrictToSources && restrictToSources.length > 0;
    // Ignore group.panel_candidate when we're designating a panel for a
    // leftover subset: the candidate is computed against the whole
    // group, so its seeds would pre-fill sources already claimed by
    // another panel and would silently bounce on the per-source overlap
    // rule (config._parse_panels). Hints are still per-source, so they
    // remain useful pre-fills for the leftover subset.
    const fromCandidate = new Map<string, number | string>();
    if (!existing && !hasRestrict && group.panel_candidate) {
      for (const m of group.panel_candidate.members) {
        fromCandidate.set(m.source, m.time_key);
      }
    }
    const seeded = fromExisting.size > 0 ? fromExisting : fromCandidate;
    const inScope = hasRestrict ? new Set(restrictToSources) : null;

    const out: Record<string, MemberDraft> = {};
    for (const sn of allSources) {
      const seededKey = seeded.get(sn);
      // Server-computed hints (panels.detect_panel_member_hints). Both
      // signals are independent — file/column precedence is the
      // editor's call to make.
      const hints = group.member_hints[sn];
      const yearFromName = hints?.year_from_name ?? null;
      const detectedColumn = hints?.time_key_column ?? null;

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
        mode = "name";
        nameValue = "";
        columnValue = "";
      }

      // Selection precedence:
      //   * singleton group → the only source is implicit
      //   * restrictToSources → exactly that subset (leftovers flow)
      //   * existing/candidate seed → opt in to the seed
      //   * fallback → only sources with a hint pre-check, so the user
      //     opts unsignalled sources in explicitly
      let selected: boolean;
      if (allSources.length === 1) {
        selected = true;
      } else if (inScope) {
        selected = inScope.has(sn);
      } else if (seeded.size > 0) {
        selected = seeded.has(sn);
      } else {
        selected = yearFromName !== null || detectedColumn !== null;
      }

      out[sn] = { selected, mode, nameValue, columnValue };
    }
    return out;
  }

  let draft: Record<string, MemberDraft> = $state(buildDraft());

  function defaultPanelId(): string {
    if (existing) return existing.panel_id;
    const base =
      group.panel_candidate?.suggested_panel_id ??
      group.register_name ??
      group.group_id;
    // Adding a second panel to a register: avoid colliding with one
    // that already lives in config.panels — the duplicate-id validator
    // would bounce the save. Suffix numerically until free.
    const taken = new Set(
      (store.snapshot?.config.panels ?? []).map((p) => p.panel_id),
    );
    if (!taken.has(base)) return base;
    let i = 2;
    while (taken.has(`${base}_${i}`)) i++;
    return `${base}_${i}`;
  }
  function defaultEntityKey(): string {
    if (existing) return existing.entity_key;
    return group.panel_candidate?.suggested_entity_key ?? "";
  }

  let panelId: string = $state(defaultPanelId());
  let entityKey: string = $state(defaultEntityKey());

  // Hover- and focus-revealed help strings. Surfaced via HoverHint on
  // the label (keyboard-reachable), and mirrored as a native title= on
  // the input so screen readers announce them on focus.
  const PANEL_ID_HELP =
    "Stable identifier for this panel; used as its key in mock_data_config.json and in extract/stats output. Defaults to the register name.";
  const ENTITY_KEY_HELP =
    "Column name that identifies the entity (e.g. person/firm id) and joins members of this panel together. Must exist on every selected source.";
  let submitting = $state(false);
  let confirmingRemoval = $state(false);

  let selectedSources: string[] = $derived(
    allSources.filter((sn) => draft[sn]?.selected),
  );

  // entity_key candidates = column names present on every selected source.
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

  // Sources already claimed by another panel in this snapshot. The
  // server's per-source overlap rule (config._parse_panels) would
  // reject any save that includes one; surface it as a client-side
  // error before the request fires. Skip the panel we're currently
  // editing — overlapping with itself is the "rename" case, not a
  // conflict.
  let otherPanelsBySource: Record<string, string> = $derived.by(() => {
    const map: Record<string, string> = {};
    const panels = store.snapshot?.config.panels ?? [];
    for (const p of panels) {
      if (existing && p.panel_id === existing.panel_id) continue;
      for (const m of p.members) map[m.source] = p.panel_id;
    }
    return map;
  });
  let overlapErrors: string[] = $derived(
    selectedSources
      .filter((sn) => otherPanelsBySource[sn])
      .map((sn) => `${sn}: already in panel ${otherPanelsBySource[sn]}`),
  );

  // Client-side mirror of the server's duplicate-id rejection. Excludes
  // the panel currently being edited so renaming to the same id is a no-op
  // rather than a conflict. defaultPanelId() seeds a free id, but the
  // user can type into the field after mount, so re-check on every change.
  let panelIdCollisionError: string | null = $derived.by(() => {
    if (panelIdTrimmed.length === 0) return null;
    const panels = store.snapshot?.config.panels ?? [];
    for (const p of panels) {
      if (existing && p.panel_id === existing.panel_id) continue;
      if (p.panel_id === panelIdTrimmed) {
        return `panel_id '${panelIdTrimmed}' already in use`;
      }
    }
    return null;
  });

  let validationErrors: string[] = $derived(
    [
      selectedSources.length === 0 ? "select at least one source" : null,
      panelIdTrimmed.length === 0 ? "panel_id required" : null,
      panelIdCollisionError,
      entityKeyError,
      duplicateLiteralError,
      ...overlapErrors,
      ...builtMembers
        .filter((b) => b.error !== null)
        .map((b) => `${b.source}: ${b.error}`),
    ].filter((m): m is string => m !== null),
  );

  let canSave = $derived(!submitting && validationErrors.length === 0);

  function resetConfirm(): void {
    confirmingRemoval = false;
  }

  function setMode(sn: string, mode: TimeKeyMode): void {
    if (draft[sn].mode === mode) return;
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

    <div class="modal-body">
      <label class="row">
        <HoverHint lines={[PANEL_ID_HELP]}>
          <span>panel_id</span>
        </HoverHint>
        <input
          type="text"
          bind:value={panelId}
          spellcheck="false"
          title={PANEL_ID_HELP}
          onfocus={resetConfirm}
          oninput={resetConfirm}
          placeholder={existing
            ? existing.panel_id
            : group.panel_candidate?.suggested_panel_id ?? ""}
        />
      </label>

      <label class="row">
        <HoverHint lines={[ENTITY_KEY_HELP]}>
          <span>entity_key</span>
        </HoverHint>
        {#if entityKeyOptions.length > 0}
          <select
            bind:value={entityKey}
            title={ENTITY_KEY_HELP}
            onfocus={resetConfirm}
            onchange={resetConfirm}
          >
            {#if entityKey && !entityKeyOptions.includes(entityKey)}
              <option value={entityKey} disabled
                >{entityKey} (missing on a member)</option
              >
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
            title={ENTITY_KEY_HELP}
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
          {#each allSources as sn, idx (sn)}
            {@const d = draft[sn]}
            {@const cols = group.columns_by_source[sn] ?? []}
            {@const otherPanel = otherPanelsBySource[sn]}
            {@const timeKeyLabelId = `time-key-label-${idx}`}
            <li class:disabled={!d.selected}>
              <label class="source-row">
                {#if allSources.length > 1}
                  <input
                    type="checkbox"
                    checked={d.selected}
                    onchange={() => toggleSource(sn)}
                  />
                {/if}
                <span class="mono source-name">{sn}</span>
                {#if otherPanel}
                  <span
                    class="overlap-tag"
                    title={`Already a member of panel ${otherPanel}; remove it there first to move it here.`}
                  >
                    in {otherPanel}
                  </span>
                {/if}
              </label>
              {#if d.selected}
                <div class="time-key-block">
                  <span class="field-label" id={timeKeyLabelId}>time_key</span>
                  <div
                    class="mode-toggle"
                    role="radiogroup"
                    aria-labelledby={timeKeyLabelId}
                  >
                    {#each MODE_OPTIONS as opt (opt.value)}
                      <label class:active={d.mode === opt.value}>
                        <input
                          type="radio"
                          name={`mode-${sn}`}
                          value={opt.value}
                          checked={d.mode === opt.value}
                          onchange={() => setMode(sn, opt.value)}
                        />
                        {opt.label}
                      </label>
                    {/each}
                  </div>
                  {#if d.mode === "name"}
                    <input
                      type="text"
                      inputmode="numeric"
                      pattern="-?\d+"
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
    </div>

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
  /* form + .modal-body flex/scroll layout is defined in Modal.svelte. */
  .field-label {
    color: #666;
    font-size: 0.78rem;
    text-transform: uppercase;
    letter-spacing: 0.04em;
  }
  .overlap-tag {
    background: #fdecd2;
    color: #7a4a14;
    border: 1px solid #f0c14b;
    border-radius: 3px;
    padding: 0.05rem 0.35rem;
    font-size: 0.72rem;
    flex: 0 0 auto;
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
  /* Keep radios in the focus tree (a11y) while letting the label act
     as the visual control. Focus ring is hoisted to the label. */
  .mode-toggle input {
    position: absolute;
    width: 1px;
    height: 1px;
    margin: -1px;
    padding: 0;
    overflow: hidden;
    clip: rect(0 0 0 0);
    white-space: nowrap;
    border: 0;
  }
  .mode-toggle label:focus-within {
    outline: 2px solid #1656c0;
    outline-offset: 1px;
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
