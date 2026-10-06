-- A year-independent state must not carry dates.
-- Fixture state 3 (TestCol, 2020) is rewritten; the DDL CHECK is bypassed so the
-- validator, not the schema, judges the row.
UPDATE variable_state SET period_scope = 'year_independent', valid_from = '2020-01-01', valid_to = '2020-12-31', pooled = 0 WHERE state_id = 3;
