-- A year-independent state with no dates and pooled = 0 is well formed.
-- Fixture state 3 (TestCol, 2020) is rewritten; the DDL CHECK is bypassed so the
-- validator, not the schema, judges the row.
UPDATE variable_state SET period_scope = 'year_independent', valid_from = NULL, valid_to = NULL, pooled = 0 WHERE state_id = 3;
