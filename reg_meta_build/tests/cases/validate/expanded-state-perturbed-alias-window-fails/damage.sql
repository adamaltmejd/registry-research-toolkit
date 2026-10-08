-- An alias window changes under derived tables that were not recomputed: variable
-- 2's TestKolumn window (a source window replacing state 3's base) now ends in June.
-- expanded_state and browse_delivery still hold the full-year window, so both fail
-- located at variable 2. Fails if either check stops recomputing from the core
-- graph, or compares row counts instead of rows.
UPDATE variable_alias_window SET valid_to = '2020-06-30'
WHERE variable_id = 2 AND delivery_column_name = 'TestKolumn';
