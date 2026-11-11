/** The entity glyphs, on the pill's 14 px box (maintainer, #2399,
 * 2026-11-11): a table is a grid, a register two stacked sheets, and a
 * variable, a variable group and a column one column in a frame; a steward's
 * database a cylinder (#2110).
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
  database: (
    <>
      <ellipse cx="7" cy="3.6" rx="4.5" ry="1.8" />
      <path d="M2.5 3.6v6.8c0 1 2 1.8 4.5 1.8s4.5-.8 4.5-1.8V3.6M2.5 7c0 1 2 1.8 4.5 1.8S11.5 8 11.5 7" />
    </>
  ),
};
