-- ÅÄÖVar (testreg/aaocol) loses its common name AND its only state (4), so it
-- ships with no name anywhere. Rows that hang off the state go with it, so only
-- the missing name is at fault.
UPDATE variable SET name = NULL WHERE variable_id = 3;
DELETE FROM variable_alias_window WHERE variable_id = 3;
DELETE FROM state_classification WHERE state_id = 4;
DELETE FROM variable_state_lineage WHERE consumer_state_id = 4 OR source_state_id = 4;
DELETE FROM variable_state_lineage_warning WHERE consumer_state_id = 4;
DELETE FROM variable_state WHERE state_id = 4;
