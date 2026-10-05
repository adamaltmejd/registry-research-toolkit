-- Damage: variable 2's exact source windows over state 3 lose the window for
-- the state's own column (TestCol); only the replacement TestKolumn remains.
DELETE FROM variable_alias_window WHERE variable_id = 2 AND delivery_column_name = 'TestCol';
