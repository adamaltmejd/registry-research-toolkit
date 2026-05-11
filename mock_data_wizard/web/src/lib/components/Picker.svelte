<script lang="ts" generics="T">
  /**
   * Reusable picker used by ValueCodesModal across all variance tiers.
   * Renders as a row of chips when the option count fits, collapses to
   * a native <select> dropdown beyond a threshold so the popup doesn't
   * grow unbounded for columns with many year-versions.
   *
   * The `eqKey` callback resolves both the option identity (for
   * comparison against `value`) and the form element value when the
   * picker collapses to a <select>. Stringified because <select> values
   * are strings; the caller maps back through `options` on change.
   */
  interface Props {
    /** Picker label, e.g. "Classification:" or "Value-set:". */
    label: string;
    /** Available options. Empty → component renders nothing. */
    options: T[];
    /** Currently selected option, or null. Compared via `eqKey`. */
    value: T | null;
    /** Display text for an option (chip body / dropdown row). */
    optionLabel: (option: T) => string;
    /** Stable identity for an option. Must be unique across `options`. */
    eqKey: (option: T) => string;
    /** Fired with the option the user picked. */
    onPick: (option: T) => void;
    /** Optional small note rendered next to the picker (e.g. "applies
     * to: cvid 1001 (2020)") describing the *currently selected* option. */
    activeDescription?: string | null;
    /** Threshold beyond which chips collapse to a <select>. Default 4
     * — picked to match the user's preference (see issue #64 follow-up). */
    collapseAt?: number;
  }

  let {
    label,
    options,
    value,
    optionLabel,
    eqKey,
    onPick,
    activeDescription = null,
    collapseAt = 4,
  }: Props = $props();

  function isActive(opt: T): boolean {
    return value !== null && eqKey(opt) === eqKey(value);
  }

  function handleSelectChange(event: Event): void {
    const target = event.currentTarget as HTMLSelectElement;
    const picked = options.find((o) => eqKey(o) === target.value);
    if (picked !== undefined) onPick(picked);
  }
</script>

{#if options.length > 0}
  <div class="picker">
    <span class="picker-label">{label}</span>
    {#if options.length > collapseAt}
      <select
        class="picker-select"
        value={value !== null ? eqKey(value) : ""}
        onchange={handleSelectChange}
      >
        {#each options as opt (eqKey(opt))}
          <option value={eqKey(opt)}>{optionLabel(opt)}</option>
        {/each}
      </select>
    {:else}
      <ul class="picker-chips">
        {#each options as opt (eqKey(opt))}
          <li>
            <button
              type="button"
              class="chip"
              class:active={isActive(opt)}
              aria-pressed={isActive(opt)}
              onclick={() => onPick(opt)}
            >
              {optionLabel(opt)}
            </button>
          </li>
        {/each}
      </ul>
    {/if}
    {#if activeDescription}
      <span class="active-desc">{activeDescription}</span>
    {/if}
  </div>
{/if}

<style>
  .picker {
    display: flex;
    align-items: center;
    gap: 0.5rem;
    margin: 0.2rem 0 0.5rem;
    flex-wrap: wrap;
  }
  .picker-label {
    color: #555;
    font-size: 0.85rem;
  }
  .picker-chips {
    list-style: none;
    padding: 0;
    margin: 0;
    display: flex;
    gap: 0.3rem;
    flex-wrap: wrap;
  }
  .chip {
    padding: 0.1rem 0.55rem;
    border: 1px solid #c8d3ec;
    border-radius: 3px;
    background: #eef2fb;
    color: #1a3b80;
    font: inherit;
    font-family: ui-monospace, monospace;
    font-size: 0.85rem;
    cursor: pointer;
  }
  .chip:hover {
    background: #e0e7f7;
  }
  .chip.active {
    background: #1a3b80;
    color: #fff;
    border-color: #1a3b80;
  }
  .chip:focus-visible {
    outline: 2px solid #1a3b80;
    outline-offset: 1px;
  }
  .picker-select {
    font: inherit;
    font-family: ui-monospace, monospace;
    font-size: 0.85rem;
    padding: 0.1rem 0.3rem;
    border: 1px solid #c8d3ec;
    border-radius: 3px;
    background: #eef2fb;
    color: #1a3b80;
    cursor: pointer;
  }
  .picker-select:focus-visible {
    outline: 2px solid #1a3b80;
    outline-offset: 1px;
  }
  .active-desc {
    color: #666;
    font-size: 0.8rem;
    flex-basis: 100%;
  }
</style>
