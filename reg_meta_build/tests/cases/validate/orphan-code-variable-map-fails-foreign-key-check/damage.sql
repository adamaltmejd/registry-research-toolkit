-- A code mapping whose owning variable does not exist.
UPDATE code_variable_map SET variable_id = 999 WHERE code_id = 0 AND variable_id = 1;
