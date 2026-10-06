-- Variable 2's exact window over state 3 for TestKolumn is a curated (errata)
-- correction, so it needs no window for the state's own column (TestCol).
DELETE FROM variable_alias_window WHERE variable_id = 2 AND delivery_column_name = 'TestCol';
UPDATE variable_alias_window SET provenance = 'errata:test
held' WHERE variable_id = 2 AND delivery_column_name = 'TestKolumn';
