<script lang="ts">
  import type { RegisterEntry } from "../types";

  /**
   * Typeahead combobox for picking a register. Replaces the native
   * `<input list>`/`<datalist>` because (a) browsers cap visible
   * suggestions in opaque ways, (b) we want to show register IDs and
   * substring matches the server already accepts, and (c) the keyboard
   * affordances (arrow nav, enter, escape) are inconsistent across
   * vendors. Server is still the source of truth — this component does
   * not validate the input, it only helps the user pick.
   */

  interface Props {
    /** All registers available to pick from. May be empty when regmeta
     * is unavailable; the component falls back to free-text in that case. */
    registers: RegisterEntry[];
    /** Two-way bound text — accept both `register_id` and full names so
     * the substring/id paths the server accepts also work here. */
    value: string;
    /** Optional callback fired whenever `value` changes. */
    oninput?: () => void;
    /** Marked invalid by the parent. */
    ariaInvalid?: boolean;
    /** Forwarded to the input for accessibility. */
    ariaDescribedby?: string;
    /** Placeholder text. */
    placeholder?: string;
  }

  let {
    registers,
    value = $bindable(""),
    oninput,
    ariaInvalid = false,
    ariaDescribedby,
    placeholder = "(none — clear)",
  }: Props = $props();

  // Suggestions cap: at 200+ registers, rendering all of them stalls
  // the open animation. 50 is enough that any non-degenerate substring
  // narrows to it; bare-empty input already shows the first 50 — useful
  // because we want users to *see* what's available even before typing.
  const MAX_OPTIONS = 50;

  let inputEl: HTMLInputElement | undefined = $state();
  let listEl: HTMLUListElement | undefined = $state();
  let open = $state(false);
  // Highlight index in `options`. -1 means "no selection yet"; arrow
  // keys move it. Reset whenever `options` changes (otherwise a stale
  // index could point past the new list).
  let highlight = $state(-1);

  let options = $derived.by<RegisterEntry[]>(() => {
    if (registers.length === 0) return [];
    const q = value.trim().toLowerCase();
    // Numeric input → exact id match wins; show that one entry first.
    const asNum = Number(q);
    const idMatch =
      Number.isInteger(asNum) && q !== ""
        ? registers.find((r) => r.id === asNum)
        : undefined;
    if (q === "") {
      return registers.slice(0, MAX_OPTIONS);
    }
    const matches: RegisterEntry[] = [];
    if (idMatch) matches.push(idMatch);
    for (const r of registers) {
      if (r === idMatch) continue;
      if (r.name.toLowerCase().includes(q)) matches.push(r);
      if (matches.length >= MAX_OPTIONS) break;
    }
    return matches;
  });

  // Reset highlight whenever the option list changes shape — the user
  // expects the first match to be the implicit default after typing.
  $effect(() => {
    void options;
    highlight = options.length > 0 ? 0 : -1;
  });

  function handleInput(event: Event) {
    value = (event.target as HTMLInputElement).value;
    open = true;
    oninput?.();
  }

  // Pending blur-driven close. Tracked so a quick blur→focus within the
  // delay can cancel the close — otherwise the dropdown flickers shut
  // even though the input is focused again.
  let blurCloseTimer: ReturnType<typeof setTimeout> | null = null;

  function handleFocus() {
    if (blurCloseTimer !== null) {
      clearTimeout(blurCloseTimer);
      blurCloseTimer = null;
    }
    open = true;
  }

  function handleBlur(event: FocusEvent) {
    // Click on a suggestion fires blur before click; defer the close so
    // the click handler can still run. relatedTarget covers keyboard
    // focus moves (Tab) without the timeout race.
    const next = event.relatedTarget as Node | null;
    if (next && listEl?.contains(next)) return;
    if (blurCloseTimer !== null) clearTimeout(blurCloseTimer);
    blurCloseTimer = setTimeout(() => {
      blurCloseTimer = null;
      open = false;
    }, 100);
  }

  function selectOption(r: RegisterEntry) {
    value = r.name;
    open = false;
    oninput?.();
    inputEl?.focus();
  }

  function handleKeydown(event: KeyboardEvent) {
    if (event.key === "ArrowDown") {
      event.preventDefault();
      open = true;
      if (options.length > 0) {
        highlight = (highlight + 1) % options.length;
      }
      return;
    }
    if (event.key === "ArrowUp") {
      event.preventDefault();
      open = true;
      if (options.length > 0) {
        highlight = (highlight - 1 + options.length) % options.length;
      }
      return;
    }
    if (event.key === "Enter") {
      if (open && highlight >= 0 && highlight < options.length) {
        event.preventDefault();
        selectOption(options[highlight]);
      }
      // Otherwise let the form submit normally — the user typed a
      // name/id directly and pressed Enter to commit it.
      return;
    }
    if (event.key === "Escape") {
      if (open) {
        event.preventDefault();
        event.stopPropagation();
        open = false;
      }
    }
  }
</script>

<div class="combobox">
  <input
    bind:this={inputEl}
    type="text"
    role="combobox"
    aria-expanded={open}
    aria-autocomplete="list"
    aria-controls="register-combobox-list"
    aria-invalid={ariaInvalid}
    aria-describedby={ariaDescribedby}
    {placeholder}
    spellcheck="false"
    {value}
    oninput={handleInput}
    onfocus={handleFocus}
    onblur={handleBlur}
    onkeydown={handleKeydown}
  />
  {#if open && options.length > 0}
    <ul
      bind:this={listEl}
      id="register-combobox-list"
      role="listbox"
      class="suggestions"
      tabindex="-1"
    >
      {#each options as r, i (r.id)}
        <li
          role="option"
          class:active={i === highlight}
          aria-selected={i === highlight}
          onmousedown={(e) => {
            // mousedown so we land before the input's blur-driven close.
            e.preventDefault();
            selectOption(r);
          }}
          onmouseenter={() => (highlight = i)}
        >
          <span class="r-name">{r.name}</span>
          <span class="r-id">#{r.id}</span>
        </li>
      {/each}
      {#if registers.length > options.length}
        <li class="more">
          showing {options.length} of {registers.length} — type to narrow
        </li>
      {/if}
    </ul>
  {/if}
</div>

<style>
  .combobox {
    position: relative;
    display: block;
  }
  input {
    width: 100%;
    padding: 0.35rem 0.5rem;
    border: 1px solid #ccc;
    border-radius: 4px;
    font: inherit;
    box-sizing: border-box;
  }
  input[aria-invalid="true"] {
    border-color: #c44;
    outline-color: #c44;
  }
  .suggestions {
    position: absolute;
    top: calc(100% + 2px);
    left: 0;
    right: 0;
    z-index: 10;
    background: #fff;
    border: 1px solid #d0d0d0;
    border-radius: 4px;
    box-shadow: 0 4px 12px rgba(0, 0, 0, 0.08);
    margin: 0;
    padding: 0.2rem 0;
    list-style: none;
    max-height: 16rem;
    overflow-y: auto;
  }
  .suggestions li {
    display: flex;
    align-items: baseline;
    gap: 0.6rem;
    padding: 0.3rem 0.6rem;
    cursor: pointer;
    font-size: 0.9rem;
  }
  .suggestions li.active {
    background: #eef2fb;
  }
  .suggestions li.more {
    cursor: default;
    color: #888;
    font-size: 0.8rem;
    border-top: 1px dashed #e0e0e0;
    margin-top: 0.2rem;
    padding-top: 0.4rem;
  }
  .r-name {
    flex: 1 1 auto;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }
  .r-id {
    flex: 0 0 auto;
    color: #888;
    font-family: ui-monospace, monospace;
    font-size: 0.78rem;
  }
</style>
