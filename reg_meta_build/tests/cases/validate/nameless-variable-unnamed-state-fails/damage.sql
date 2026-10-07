-- Two variables lose their common name. Kon (testreg) names state 1 but state 2
-- holds only a no-break space (char(160)): `str.strip()` treats it as blank,
-- SQLite TRIM() would not, so this pins the Unicode-whitespace definition.
-- TestVar's only state (3) has no name at all. Each must be located with its
-- unnamed state ids.
UPDATE variable SET name = NULL WHERE variable_id IN (1, 2);
UPDATE variable_state SET name = 'Kön' WHERE state_id = 1;
UPDATE variable_state SET name = char(160) WHERE state_id = 2;
