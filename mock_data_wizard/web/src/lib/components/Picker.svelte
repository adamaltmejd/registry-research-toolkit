<script lang="ts" generics="T">
  interface Props {
    label: string;
    options: T[];
    value: T | null;
    optionLabel: (option: T) => string;
    /** Stable identity for an option. Used both for `value` comparison
     * and as the <select> form value (stringified). */
    eqKey: (option: T) => string;
    onPick: (option: T) => void;
    /** Per-option hover tooltip. Returns null when the option has no
     * description (the title attribute is then omitted). In chip mode
     * every chip carries its own tooltip; in select mode the tooltip
     * tracks the currently active option (browsers don't render
     * <option> titles reliably). */
    optionTooltip?: (option: T) => string | null;
    /** Chip count above which the picker collapses to a <select>. */
    collapseAt?: number;
  }

  let {
    label,
    options,
    value,
    optionLabel,
    eqKey,
    onPick,
    optionTooltip,
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
        title={(value !== null && optionTooltip?.(value)) || undefined}
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
              title={optionTooltip?.(opt) || undefined}
            >
              {optionLabel(opt)}
            </button>
          </li>
        {/each}
      </ul>
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
</style>
