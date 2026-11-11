/** The entity glyphs, on the pill's 14 px box (maintainer, #2399,
 * 2026-11-11): a table is a grid, a register two stacked sheets, and a
 * variable, a variable group and a column one column in a frame.
 * Decorative: the name or word beside it carries the meaning. */
export const ENTITY_GLYPHS = {
  table: (
    <>
      <rect x="1.5" y="2.5" width="11" height="9" rx="1" />
      <path d="M1.5 5.5h11M1.5 8.5h11M5.5 5.5v6M9.5 5.5v6" />
    </>
  ),
  register: (
    <>
      <path d="M4 4.5V2.5a1 1 0 0 1 1-1h7a1 1 0 0 1 1 1v6a1 1 0 0 1-1 1h-1.5" />
      <rect x="1" y="4.5" width="9.5" height="8" rx="1" />
      <rect x="1" y="4.5" width="9.5" height="2.5" fill="currentColor" />
    </>
  ),
  column: (
    <>
      <rect x="1.5" y="2.5" width="11" height="9" rx="1" />
      <rect x="5.5" y="2.5" width="3" height="9" fill="currentColor" />
    </>
  ),
};
